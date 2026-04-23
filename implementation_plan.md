# Kế hoạch Nâng cấp Hệ thống Fraud Detection Pipeline

Tài liệu này mô tả chi tiết kế hoạch 4 bước để kết hợp các điểm mạnh từ dự án `fraud-detection-example` (Kiến trúc Medallion, Delta Lake, Hybrid Scoring, Business Rules) vào dự án `fraud-detection-pipeline` hiện tại của bạn.

## User Review Required

> [!IMPORTANT]
> **Thay đổi định dạng lưu trữ**: Nâng cấp lên Delta Lake sẽ yêu cầu kéo thêm package Maven `io.delta:delta-spark` mỗi lần chạy `spark-submit`.
> **Kiến trúc Medallion**: Việc lưu dữ liệu thành 3 tầng (Bronze, Silver, Gold) trong cùng 1 micro-batch (`foreachBatch`) sẽ làm tăng thời gian xử lý I/O (ghi HDFS 3 lần thay vì 1 lần).

## Open Questions

> [!WARNING]
> 1. Spark của bạn đang là version `3.5.3`. Tôi sẽ dùng `io.delta:delta-spark_2.12:3.3.0` (tương thích Spark 3.5.x). Bạn có đồng ý với version này không?
> 2. Trọng số Hybrid Score tôi đề xuất là `0.7 * ML + 0.3 * Rule`. Ngưỡng cảnh báo là `Score >= 0.75` hoặc `Rule chạm mức nghiêm trọng (Blacklist)`. Bạn có muốn điều chỉnh công thức này không?

---

## Proposed Changes

### 1. Mở rộng Tập Luật Nghiệp vụ (Business Rules)

Sử dụng các feature đã có sẵn (`is_zero_balance_orig`, `is_large_amount`) để làm luật cứng song song với Blacklist.

#### [MODIFY] `spark/streaming_pipeline.py`
- Cập nhật hàm `add_rule_based_alerts` để nhận diện thêm các luật:
  - `RULE_DRAIN`: Giao dịch rút cạn tiền (`is_zero_balance_orig == 1.0`).
  - `RULE_LARGE_TXN`: Giao dịch > 200,000 (`is_large_amount == 1.0`).
  - `RULE_BLACKLIST`: Tài khoản đích nằm trong blacklist.

---

### 2. Triển khai Chấm điểm Lai (Hybrid Scoring)

Kết hợp điểm xác suất của ML Model và điểm của Rule để ra một con số cuối cùng (Hybrid Score).

#### [MODIFY] `spark/streaming_pipeline.py`
- Trong hàm `process_batch`, tính toán cột `hybrid_score`.
  - Giả sử: `rule_score` = Tổng trọng số các luật vi phạm (VD: Blacklist = 1.0, Drain = 0.5, Large TXN = 0.5, max = 1.0).
  - `hybrid_score = (fraud_probability * 0.7) + (rule_score * 0.3)`.
- Đánh dấu `is_alert = 1` nếu `hybrid_score >= 0.75` hoặc chạm `RULE_BLACKLIST`.
- Thêm `hybrid_score` vào cấu trúc JSON đẩy lên Redis để Frontend (FastAPI) hiển thị.

---

### 3. Nâng cấp Parquet lên Delta Lake

Thay thế định dạng lưu trữ từ Parquet sang Delta Lake để hỗ trợ ACID và Time Travel.

#### [MODIFY] `spark/streaming_pipeline.py`
- Sửa hàm `main()` để cấu hình `SparkSession` hỗ trợ Delta Lake:
  ```python
  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
  ```
- Sửa lệnh ghi HDFS trong `process_batch` từ `.format("parquet")` sang `.format("delta")`.

#### [MODIFY] `scripts/start_pipeline.sh`
- Thêm package Delta Lake vào lệnh `spark-submit` trong phần hướng dẫn chạy (echo) để bạn copy paste cho đúng.

---

### 4. Áp dụng Kiến trúc Medallion (Bronze / Silver / Gold)

Chia dữ liệu lưu trữ trên HDFS thành 3 zone rõ rệt. Vì chúng ta đang dùng `foreachBatch`, ta có thể lưu dữ liệu ở các giai đoạn khác nhau trong cùng 1 hàm.

#### [MODIFY] `scripts/start_pipeline.sh`
- Tạo thêm các thư mục trên HDFS:
  - `/datalake/bronze` (Lưu raw data)
  - `/datalake/silver` (Lưu clean & feature data)
  - `/datalake/gold` (Lưu alerts & scored data)

#### [MODIFY] `spark/streaming_pipeline.py`
- Thay đổi cấu trúc hàm `process_batch`:
  - Lệnh ghi 1: Ghi `batch_df` (Dữ liệu gốc từ Kafka) vào thư mục `HDFS_BRONZE`.
  - Lệnh ghi 2: Ghi `featured_df` (Dữ liệu sau khi làm sạch & thêm feature) vào thư mục `HDFS_SILVER`.
  - Lệnh ghi 3: Ghi `output_df` (Dữ liệu đã có `prediction`, `hybrid_score`, `is_alert`) vào thư mục `HDFS_GOLD`.

---

## Verification Plan

### Automated Tests
- Chạy lại `pytest tests/test_preprocessing.py -v` (nếu có) để đảm bảo không gãy pipeline hiện tại.

### Manual Verification
1. Khởi động lại cụm Docker: `./scripts/start_pipeline.sh`.
2. Chạy streaming với tham số package mới:
   `docker exec spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,io.delta:delta-spark_2.12:3.3.0 /opt/spark/work/spark/streaming_pipeline.py`
3. Chạy producer đẩy dữ liệu.
4. Kiểm tra HDFS:
   - `docker exec namenode hdfs dfs -ls /datalake/bronze`
   - `docker exec namenode hdfs dfs -ls /datalake/silver`
   - `docker exec namenode hdfs dfs -ls /datalake/gold`
5. Kiểm tra Redis (hoặc API `/recent-alerts`) xem payload alert đã có trường `hybrid_score` chưa.
