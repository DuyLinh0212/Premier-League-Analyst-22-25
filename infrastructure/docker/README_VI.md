# Đóng gói và triển khai

`compose.yml` chạy các image ứng dụng và kết nối tới Neon PostgreSQL, Kafka, HDFS và Airflow bên ngoài. Compose production không dựng Hadoop, Kafka, Airflow hay database. Nếu máy đã có image ứng dụng tương thích, cấu hình `RUNTIME_IMAGE`, `API_IMAGE`, `WEB_IMAGE`; nếu chưa có, build từ các Dockerfile trong thư mục này.

Tạo `.env` từ `.env.example` ở root và cấu hình địa chỉ dịch vụ. Giữ thông tin đăng nhập trong `.env`; không commit file này. `NeonKey.txt` là nguồn bí mật riêng trên máy phát triển và không được đưa vào gói bàn giao.

```powershell
# Chỉ tạo file khi chưa có, giữ nguyên cấu hình cá nhân hiện hữu.
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
# Sau khi cấu hình, tại root project:
docker compose --env-file .env -f compose.yml config --quiet
```

Các launcher tại `infrastructure/scripts/launchers/` chạy frontend/backend và khởi động container Kafka/Airflow hiện có bằng `docker start`. Chúng không tạo container. `compose.dev.yml` và `compose.bigdata.yml` là fixture dành cho CI trên GitHub Actions; không dùng các file đó để dựng Hadoop cục bộ.

## Airflow và HDFS

DAG nằm trong `orchestration/dags`. Airflow worker/DAG processor cần mount mã dự án tại `/opt/project`, cài Python 3.11 cùng requirements runtime, Java 17, Spark 3.5.1/Scala 2.12 và connector từ `infrastructure/docker/jars`. Cấu hình `PYTHONPATH`, `SPARK_PYTHON`, `HDFS_BIN`, `HDFS_URI`, `NAMENODE_HTTP_URI` và `NEON_DATABASE_URL` trong môi trường container hiện có. HDFS giữ Bronze/Silver/Gold; Neon nhận các báo cáo hoàn chỉnh cùng thông tin lần chạy.

Dockerfile runtime lấy Hadoop client từ Spark; không tải Hadoop server. Pipeline kiểm tra NameNode và số DataNode trước khi ghi. Không format HDFS hoặc khởi tạo lại dữ liệu trên máy đang chạy.

## Gói bàn giao

```bash
python infrastructure/scripts/build/package_portable.py
python infrastructure/docker/verify_bundle.py --archive artifacts/PremierLeague_Docker.zip
```

ZIP gồm mã nguồn, dữ liệu mẫu, model, dependency và manifest SHA-256; loại `.env`, `NeonKey.txt`, cache, image tar và Hadoop archive. Việc publish lên Neon từ GitHub Actions cần repository secret `NEON_DATABASE_URL`.
