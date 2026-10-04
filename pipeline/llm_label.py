"""BONUS — an LLM inside the pipeline (slide "LLM là một bước transform").

The support team wants an LLM pre-triage label on every live ticket
(gold_ticket_labels), to compare with the human `category` and to triage new
tickets faster. An LLM step is a transform like any other — except it is
expensive, slow and NOT deterministic, so the slide's four rules apply:

  1. key = hash(input) + model + prompt version  -> a re-run makes 0 LLM calls;
     changing the prompt re-labels everything ON PURPOSE
  2. force a structured output, validate it; invalid -> quarantine, never Gold
  3. estimate the cost BEFORE running (rows x tokens x price)
  4. LLM labels are versioned data (model + prompt_version stored on every row)

The shipped `label_tickets` is the NAIVE version: it calls the model for every
ticket on every run and writes whatever comes back. Your bonus task is to make
`python -m scripts.bonus_llm` print BONUS PASS. Zero-key: `FakeLLM` stands in for a
real model (swap in any provider via .env if you like — the pipeline is the same).
"""
from __future__ import annotations

import json
import re
from hashlib import sha256

import duckdb

MODEL = "fake-llm-2026-09"
PROMPT_VERSION = "triage-v1"
ALLOWED_LABELS = ("bug", "billing", "other")
PRICE_PER_1K_TOKENS_USD = 0.002          # pretend price, for the cost estimate


PROMPT_TEMPLATE = """You triage customer-support tickets.
Answer ONLY with JSON: {{"label": "bug" | "billing" | "other"}}.
Ticket: {text}"""


class FakeLLM:
    """Deterministic stand-in for a chat model. Counts calls and tokens."""

    def __init__(self, model: str = MODEL) -> None:
        self.model = model
        self.calls = 0
        self.tokens = 0

    def complete(self, prompt: str) -> str:
        self.calls += 1
        self.tokens += len(prompt.split()) + 8
        text = prompt.lower()
        if "xuất" in text:
            return 'Sure! Here is the label: {"label": "export"}'   # off-schema answer
        if re.search(r"crash|lỗi|sso|đăng nhập|chatbot", text):
            return '{"label": "bug"}'
        if re.search(r"tiền|hoá đơn|thanh toán|gói|vat", text):
            return '{"label": "billing"}'
        return '{"label": "other"}'


def estimate_tokens(texts: list[str]) -> int:
    return sum(len(PROMPT_TEMPLATE.format(text=t).split()) + 8 for t in texts)


def validate_label_response(raw: str) -> tuple[str | None, str | None]:
    """Validate the exact JSON schema expected from the model."""
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None, "invalid_json"
    if not isinstance(payload, dict) or set(payload) != {"label"}:
        return None, "schema_mismatch"
    label = payload["label"]
    if not isinstance(label, str):
        return None, "label_must_be_string"
    if label not in ALLOWED_LABELS:
        return None, "label_not_allowed"
    return label, None


def parse_label(raw: str) -> str | None:
    """Return a label only when the response matches the complete schema."""
    label, _ = validate_label_response(raw)
    return label


def live_tickets(con: duckdb.DuckDBPyConnection) -> list[tuple[str, str]]:
    return con.execute("""
        SELECT ticket_id, subject || '. ' || body AS text
        FROM silver_tickets
        WHERE NOT is_deleted
        ORDER BY ticket_id
    """).fetchall()


def _input_hash(text: str) -> str:
    return sha256(text.encode("utf-8")).hexdigest()


def _cache_key(input_hash: str, model: str, prompt_version: str) -> str:
    material = "\0".join((input_hash, model, prompt_version))
    return sha256(material.encode("utf-8")).hexdigest()


def label_tickets(con: duckdb.DuckDBPyConnection, llm: FakeLLM) -> dict:
    """Cache versioned model responses; invalid output is quarantined, never Gold."""
    con.execute("""CREATE TABLE IF NOT EXISTS llm_label_cache (
        cache_key VARCHAR PRIMARY KEY, input_hash VARCHAR NOT NULL,
        model VARCHAR NOT NULL, prompt_version VARCHAR NOT NULL,
        label VARCHAR, raw_response VARCHAR NOT NULL, validation_error VARCHAR)""")
    con.execute("""CREATE TABLE IF NOT EXISTS llm_label_quarantine (
        ticket_id VARCHAR, input_hash VARCHAR, model VARCHAR,
        prompt_version VARCHAR, raw_response VARCHAR, reason VARCHAR)""")

    cache = {
        key: (label, raw, error)
        for key, label, raw, error in con.execute(
            """SELECT cache_key, label, raw_response, validation_error
               FROM llm_label_cache WHERE model = ? AND prompt_version = ?""",
            [llm.model, PROMPT_VERSION],
        ).fetchall()
    }
    gold_rows = []
    quarantine_rows = []
    cache_hits = 0
    calls_before = llm.calls
    for ticket_id, text in live_tickets(con):
        input_hash = _input_hash(text)
        key = _cache_key(input_hash, llm.model, PROMPT_VERSION)
        if key in cache:
            label, raw, error = cache[key]
            cache_hits += 1
        else:
            raw = llm.complete(PROMPT_TEMPLATE.format(text=text))
            label, error = validate_label_response(raw)
            con.execute(
                """INSERT INTO llm_label_cache
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                [key, input_hash, llm.model, PROMPT_VERSION, label, raw, error],
            )
            cache[key] = (label, raw, error)

        if error is None:
            gold_rows.append((ticket_id, label, llm.model, PROMPT_VERSION))
        else:
            quarantine_rows.append((ticket_id, input_hash, llm.model,
                                    PROMPT_VERSION, raw, error))

    # Each invocation publishes only this prompt version's valid current labels.
    con.execute("""CREATE OR REPLACE TABLE gold_ticket_labels (
        ticket_id VARCHAR, label VARCHAR, model VARCHAR, prompt_version VARCHAR)""")
    if gold_rows:
        con.executemany("INSERT INTO gold_ticket_labels VALUES (?, ?, ?, ?)", gold_rows)

    con.execute("DELETE FROM llm_label_quarantine WHERE model = ? AND prompt_version = ?",
                [llm.model, PROMPT_VERSION])
    if quarantine_rows:
        con.executemany("INSERT INTO llm_label_quarantine VALUES (?, ?, ?, ?, ?, ?)",
                        quarantine_rows)
    return {"labeled": len(gold_rows), "calls": llm.calls,
            "cache_hits": cache_hits, "quarantined": len(quarantine_rows),
            "calls_this_run": llm.calls - calls_before}
