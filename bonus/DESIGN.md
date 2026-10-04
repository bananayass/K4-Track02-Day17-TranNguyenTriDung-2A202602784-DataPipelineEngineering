# Bonus B2 — Data flywheel cho chatbot CSKH 

## Bài toán và ràng buộc

Thiết kế này nhắm tới một sản phẩm AI hỗ trợ khách hàng cho doanh nghiệp SaaS Việt Nam. Khách hàng hỏi qua web/app, nhân viên trả lời trong CRM, còn trạng thái ticket và đánh giá sau hỗ trợ đến từ các hệ thống khác nhau. Chatbot cần phân loại yêu cầu, tìm câu trả lời trong tài liệu sản phẩm, đề xuất bước xử lý cho nhân viên và học từ phản hồi đã được kiểm duyệt.

Đây là thiết kế cho giai đoạn pilot, chưa phải mô tả hệ thống production đang chạy. Các ràng buộc cần kiểm chứng gồm: bản ghi có thể trùng hoặc đến muộn; schema CRM thay đổi theo phiên bản; transcript có tên, email, số điện thoại và thông tin tài khoản; văn bản có tiếng Việt có dấu, không dấu và tiếng Anh xen kẽ; một lượt tương tác có thể kéo dài qua nhiều ngày. Nhãn đúng thường chỉ biết sau khi nhân viên xử lý xong. Dữ liệu thô cần giữ đủ để điều tra và backfill, nhưng không được cấp quyền đọc rộng vì lý do đó.

## Phiên brainstorm: các quyết định cần kiểm chứng

### 1. Nguồn và hình dạng

**Câu hỏi:** Làm sao ghép ticket, click, transcript và tài liệu hướng dẫn khi mỗi hệ thống có ID, thời gian và schema riêng?

**Quyết định:** Dùng CDC cho trạng thái ticket, Kafka cho sự kiện người dùng, và batch object export cho transcript/tài liệu. Adapter nguồn chuẩn hoá thành event envelope có `entity_id`, `event_id`, `event_time`, `ingested_at`, `schema_version` và lineage nguồn. Bronze giữ payload gốc phân vùng theo ngày ingest; Silver chuẩn hoá khoá và schema theo phiên bản.

**Đánh đổi:** Adapter theo nguồn tốn công hơn một webhook JSON chung, nhưng giữ được semantics của CDC, offset Kafka và thời điểm xảy ra. Envelope chung giúp downstream đơn giản mà không giả vờ các nguồn vốn có cùng ngữ nghĩa. Schema mới phải được thêm có chủ đích; trường lạ đi quarantine thay vì bị bỏ âm thầm.

### 2. Batch hay streaming

**Câu hỏi:** Dữ liệu nào cần tươi trong vài phút, dữ liệu nào chỉ cần cho lần train ngày kế tiếp?

**Quyết định:** Dùng consumer streaming hoặc microbatch ngắn cho trạng thái routing và sự kiện mới; đặt mục tiêu pilot là cập nhật feature routing trong vòng năm phút. Tài liệu, snapshot training và đánh giá mô hình chạy batch mỗi đêm. Feature online và offline dùng cùng định nghĩa, nhưng có projection riêng để phục vụ độ trễ khác nhau.

**Đánh đổi:** Routing tươi hơn nhưng cần theo dõi consumer lag và xử lý late event; batch ít vận hành hơn nhưng ticket vừa đổi trạng thái sẽ phản ánh chậm. Không chọn Kappa cho mọi dữ liệu: chạy streaming cả transcript, backfill và training làm tăng chi phí, retry và độ phức tạp mà không cải thiện use case batch. Nếu pilot chứng minh SLA năm phút không có giá trị, chuyển phần routing về microbatch dài hơn.

### 3. Hợp đồng dữ liệu và PII

**Câu hỏi:** Làm sao ngăn event hỏng hoặc dữ liệu cá nhân đi vào index/training mà không mất dấu nguyên nhân?

**Quyết định:** Validate schema tại staging: ID bắt buộc, timestamp parse được, enum hợp lệ, phiên bản schema hỗ trợ. Record sai vào quarantine kèm source/offset, reason và metric; cảnh báo theo tỷ lệ lỗi thay vì dừng cả batch. Phân loại PII ở ingest; trước khi rời vùng raw, mask email/điện thoại và dùng NER/DLP để tìm tên, địa chỉ, mã tài khoản trong tiếng Việt. Quyền truy cập và thời hạn lưu Bronze phải hẹp hơn Silver đã xử lý.

**Đánh đổi:** Quarantine một phần làm dữ liệu downstream thiếu tạm thời nhưng giữ được dịch vụ; bỏ qua lỗi làm pipeline “xanh” nhưng tạo nhãn sai. NER tăng recall so với regex nhưng có false positive và chi phí review. Chọn redaction trước Gold/vector, lưu raw bị giới hạn, và có quy trình deletion theo lineage để xử lý snapshot, cache, backup; tombstone đơn lẻ không chứng minh đã xoá mọi bản sao.

### 4. Train/serve parity và point-in-time

**Câu hỏi:** Làm sao tránh mô hình train dùng thông tin chỉ xuất hiện sau thời điểm chatbot đưa ra quyết định?

**Quyết định:** Định nghĩa feature một lần, gắn `event_time`, và tạo training row bằng point-in-time join tại thời điểm nhận yêu cầu. Phản hồi của nhân viên có thể đến muộn; đo P99 lateness từ Bronze rồi đặt lookback theo ngày bằng `ceil(P99)`. Nhãn “đã giải quyết” chỉ được tạo sau cửa sổ kết quả đã thống nhất, ví dụ bảy ngày.

**Đánh đổi:** Lưu lịch sử và dựng snapshot as-of tốn dung lượng, trong khi chỉ dùng trạng thái mới nhất rẻ hơn nhưng gây leakage và làm train/serve lệch nhau. Chọn tính đúng thời điểm vì metric offline vô nghĩa nếu feature chứa kết quả tương lai. Cần kiểm tra checksum giữa feature serving và batch sample, đồng thời tách train/test theo thời gian và khách hàng để giảm rò rỉ giữa các lượt cùng một tài khoản.

### 5. Vector, graph và cách dùng phản hồi

**Câu hỏi:** Câu hỏi của khách hàng cần tìm một đoạn hướng dẫn hay suy luận qua nhiều thực thể?

**Quyết định:** Bắt đầu bằng vector retrieval cho FAQ/tài liệu có chunk ID, phiên bản, sản phẩm và ngày hiệu lực; dùng SQL lookup cho quyền gói dịch vụ và trạng thái ticket. Chỉ thêm knowledge graph nếu đánh giá cho thấy câu hỏi multi-hop như “gói nào hỗ trợ tích hợp này cho chi nhánh đó?” thường thất bại với lookup có cấu trúc. Feedback được gắn với câu trả lời, tài liệu nguồn và phiên bản model; chỉ mẫu được nhân viên duyệt mới đi vào eval/train.

**Đánh đổi:** Vector search triển khai nhanh nhưng có thể lấy tài liệu cũ hoặc câu gần nghĩa sai; metadata filter và kiểm tra ngày hiệu lực giảm rủi ro. Graph biểu diễn quan hệ rõ hơn nhưng cần ontology và đồng bộ thêm một bản dữ liệu. Không tự động đưa mọi thumbs-down thành nhãn huấn luyện: feedback có thể do chờ lâu hoặc chính sách từ chối, không nhất thiết câu trả lời sai.

### 6. Replay, chi phí và vận hành

**Câu hỏi:** Làm sao backfill và thử prompt mới mà không gọi model lặp lại, nhân bản event hoặc làm bẩn eval set?

**Quyết định:** Dedup event theo `event_id`, CDC theo khoá và LSN; ghi stage bằng merge/overwrite partition. Cache LLM theo hash input + model + prompt version, lưu schema status; output sai vào quarantine. Phiên bản prompt/model nằm trên mỗi nhãn. Ước tính tokens/cost trước run, đặt quota theo phiên bản và chọn một sample có nhân viên review làm gold evaluation set.

**Đánh đổi:** Cache giảm chi phí và giúp replay ổn định nhưng cần invalidation rõ khi input, model hoặc prompt đổi. Nhãn tự động nhanh, human review đắt; chọn review theo rủi ro và disagreement thay vì review mọi ticket. Với người dùng Việt Nam, báo cáo riêng chất lượng cho tiếng Việt có dấu/không dấu và câu pha tiếng Anh; không suy ra chất lượng từ điểm trung bình toàn bộ.

## Kiến trúc đề xuất

```text
Web / App ── Kafka events ─────┐
CRM ──────── Debezium CDC ─────┼──> Bronze raw (Parquet, lineage, retention hạn chế)
Chat/docs ── object exports ────┘                  │
                                                   v
                                    Schema + PII gate ──> quarantine + alert
                                                   │
                                                   v
                                    Silver canonical / dedup / event time
                                      │             │              │
                           routing features   offline PIT     docs + vectors
                                      │             │              │
                                      └──────> chatbot / agent UI <─┘
                                                     │
                         resolution + reviewed feedback / eval labels
                                                     │
                                  versioned training snapshot + monitoring
```

## Đo thành công và bước tiếp theo

Trong pilot, đo routing freshness p95 dưới năm phút; tỷ lệ event hợp lệ và quarantine theo source; P99 lateness so với lookback; số replay không làm đổi checksum; PII leakage trong tập canary; grounded-answer rate theo bộ câu hỏi có tài liệu chuẩn; train/serve feature skew; và chi phí model trên mỗi ticket được giải quyết. Ngưỡng chấp nhận cần được chủ sản phẩm, privacy owner và nhóm hỗ trợ thống nhất trước khi bật tự động hoá.

Tiếp theo cần lấy mẫu transcript đã được phép dùng, phỏng vấn nhân viên về hành động thật sự giúp ích, xác nhận retention/deletion với privacy owner, rồi chạy pilot shadow mode. So sánh đề xuất với quyết định nhân viên trước khi cho chatbot gửi câu trả lời tự động. Các SLA và ngưỡng nêu trên là giả định thiết kế, cần đo lại bằng traffic thật.
