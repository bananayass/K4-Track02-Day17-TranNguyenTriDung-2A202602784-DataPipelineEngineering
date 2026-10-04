# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Trần Nguyễn Trí Dũng / 2A202602784
**Repo:** https://github.com/bananayass/K4-Track02-Day17-TranNguyenTriDung-2A202602784-DataPipelineEngineering
**Commit bài nộp:** 
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Codex hỗ trợ trong việc đọc và giải thích code cũng như chỉ xem nên làm như thế nào
**Nguồn tham khảo khác (nếu có):** README và tài liệu trong `docs/`.

## 1. Ba lỗi

Mỗi lỗi 4 dòng. Triệu chứng = thứ bạn *thấy* đầu tiên (check nào fail, số nào lạ,
checksum nào lệch) — không phải cách sửa.

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** |24 dòng/12 tickets; T-91 có 3 trạng thái |u05 08-12: (2,1,0), cần (5,3,1) |T-97 còn trong Silver, latest snapshot, 2 chunks |
| **Nguyên nhân gốc** |INSERT; không upsert/so LSN giữa batches |lookback 0; không tính lại event date cũ |Chỉ đọc after.ticket_id; null làm mất delete |
| **Cách sửa** (file, vài dòng) |silver.py: MERGE key; update khi LSN mới hơn |config.py: lookback 3; event_time là lúc xảy ra, _ingested_at là lúc Kafka nhận; Gold ghi đè [day−3,day] theo event date |staging.py: after/before/key; giữ op=d/LSN, bỏ tombstone; Silver tombstone |
| **Khái niệm trên slide** |Upsert key; LSN; idempotency |Event vs. ingest time; lookback |CDC delete vs. Kafka tombstone |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày → `LOOKBACK_DAYS = 3` (P50 `0.00`, P95 `2.90`, max `3.00`).
- `submission/checksums.txt`: PASS — Gold checksum: `39e115c510ecdf526800eac227158a4f` (C0=C1=C2=C3; feature `8630e04a61d1`, training `9370ca77af23`, chunks `cb9ebd12fdcc`).
- `make parity`: PARITY — `silver_tickets` `3c15dfd43701`; `gold_feature_daily` `8630e04a61d1` ở cả Python và dbt.

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo key/LSN giữ trạng thái mới; overwrite-partition tính lại event date.
- Tombstone giữ key/LSN chống replay; payload cá nhân đặt null.
- Snapshot "as of" giữ point-in-time; xoá production cần workflow riêng.
- DuckDB chạy local zero-key; dbt parity cùng Bronze. `unique_key` là key, `merge_update_condition` chặn LSN cũ, `batch_size=day`, `lookback=3`.

## 4. Hai câu hỏi suy ngẫm

1. Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày
   08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào?

   Lab giữ snapshot cũ; latest/RAG loại T-97 nhưng Bronze và `silver_transcripts` còn văn bản. Production theo lineage để xoá/rebuild snapshot, backup/cache hoặc crypto-shred; giữ audit phi-PII. Lab không xoá PII đầy đủ.

2. Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ
   đặt chốt PII nào, ở tầng nào, và đo nó ra sao?

   Redact/quarantine tại Bronze→Silver trước Gold/vector; thêm NER/DLP và human review. Đo precision, recall, leakage rate trên tập đa ngôn ngữ gán nhãn và canary.

## 5. Output (dán nguyên văn)

```text
$ .venv/bin/python -m pytest tests/test_contracts.py -k "one_row_per_ticket or latest_state_wins"
..                                                                       [100%]
2 passed, 11 deselected in 3.31s

$ .venv/bin/python -m pytest tests/test_contracts.py -k "feature_daily or late_events or lookback"
...                                                                      [100%]
3 passed, 10 deselected in 3.93s

$ make verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

$ make test
34 passed in 7.13s

$ make rerun3
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

$ make lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ make dbt
Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
Completed successfully
Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ make parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree

$ make bonus-llm
=== bonus: LLM labelling of 11 live tickets ===
  cost estimate before running: ~484 tokens = $0.0010 per full run
  [OK ] first run labels every live ticket
  [OK ] re-run with same model + prompt makes 0 LLM calls
  [OK ] every Gold label is bug / billing / other
  [OK ] off-schema answers go to llm_label_quarantine
  [OK ] new prompt version re-labels on purpose
  [OK ] labels carry their prompt version
BONUS PASS
```

B2 brainstorm: [bonus/DESIGN.md](../bonus/DESIGN.md)

Nếu dùng PowerShell, ghi lệnh tương đương và output thực tế theo [SUBMISSION.md](../docs/SUBMISSION.md).
Nếu làm bonus, thêm output B1 hoặc đường dẫn bằng chứng B2 ở cuối phần này.
