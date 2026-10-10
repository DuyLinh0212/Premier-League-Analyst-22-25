# Công cụ đóng gói và chuyển HDFS

Hệ thống hiện tại chạy hoàn toàn bằng Docker Compose trong portable, gồm Airflow LocalExecutor, Spark, Kafka, Neo4j, PostgreSQL và HDFS 1 NameNode + 3 DataNode. Không cần SSH hoặc runtime WSL riêng.

Xem portable/README_VI.md để khởi động và sử dụng. Model, Silver và kết quả KNN đã nằm trong gói; khởi động không yêu cầu huấn luyện lại.

- package_portable.py: tạo ZIP dữ liệu, mã nguồn, cấu hình và bằng chứng nghiệm thu; loại bí mật và bản sao lưu hệ cũ.
- export_images.py: xuất các image đã nghiệm thu thành delivery/football-images.tar.gz, ghi SHA-256 và ID image trong images_manifest.json.
- export_portable_seed.py: xuất Silver/model/KNN thành seed phục vụ bootstrap.
- migrate_blocks.py: chuyển file HDFS sang block 64 MiB, replication 2, kiểm tra SHA-256/schema/số dòng và giữ journal phục hồi.
- retire_legacy.ps1: chuyển nguồn cũ vào bản sao lưu sau khi nghiệm thu stack mới hoàn tất; kiểm tra checksum KNN được giữ.

Nguồn cũ, lịch sử Airflow và Neo4j thuộc đồ án được sao lưu ngoài gói bàn giao trong _legacy_backup trước khi dọn. Các công cụ SSH và phân tích cũ đã được nghỉ sử dụng.
