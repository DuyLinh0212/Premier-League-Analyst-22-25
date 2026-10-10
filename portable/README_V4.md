# Bàn giao ứng dụng với hạ tầng ngoài

Production Compose **chỉ chạy ứng dụng**. Máy nhận phải có Neo4j, Kafka, HDFS và Airflow đang chạy; không format NameNode, không tạo Hadoop mới. Không chạy Compose CI trên máy người dùng: CI được phép dùng dịch vụ tạm trên GitHub runner.

## Cấu hình địa chỉ

Giải nén sang đường dẫn bất kỳ. Từ thư mục gốc:

```powershell
Copy-Item portable/.env.example portable/.env
# Chỉnh portable/.env theo dịch vụ hiện có, rồi:
powershell -ExecutionPolicy Bypass -File portable/start.ps1
```

Linux:

```sh
cp portable/.env.example portable/.env
# Chỉnh .env rồi:
bash portable/start.sh
```

Script mặc định `--no-build --pull never`: tái sử dụng `RUNTIME_IMAGE`, `API_IMAGE`, `WEB_IMAGE` trong `.env`. Các image này phải tương thích với ứng dụng, Spark 3.5.1 / Scala 2.12 / Java 17 / Python 3.11. Một image bất kỳ có Airflow hoặc Spark không tự động chứa API và Web. Khi chưa có image ứng dụng, máy nhận có thể dùng `start.ps1 -Build` hoặc `start.sh --build` để build ba image ứng dụng. Nó vẫn không build hay dựng lại hạ tầng.

`NEO4J_HTTP_URI` là HTTP Query API của Neo4j 5.26 (ví dụ 7474), không phải Bolt 7687. Dùng tài khoản và Database hiện có; không ghi mật khẩu vào Git. `APP_NAMESPACE` tách dữ liệu dự án với dữ liệu hiện có. Không có lệnh xóa Database trong bootstrap.

`KAFKA_BOOTSTRAP_SERVERS` và **tất cả advertised.listeners** phải truy cập được từ container ứng dụng / worker. HDFS NameNode RPC cùng mọi địa chỉ DataNode trả về cũng phải truy cập được. `localhost` trong container chỉ container đó. Windows/WSL có thể dùng `host.docker.internal` nếu các cổng được forward và DataNode quảng bá hostname tương ứng. Client dùng `dfs.client.use.datanode.hostname=true`. Cần kiểm tra mạng thực tế trước khi chạy pipeline; cấu hình DHCP/IP của máy nhận có thể khác.

`HADOOP_USER_NAME` chỉ dùng khi HDFS ở chế độ simple authentication và đã được quản trị hạ tầng cho phép. Hệ thống Kerberos cần cung cấp cấu hình/credential riêng của hạ tầng hiện có. `NAMENODE_HTTP_URI` trỏ HTTP JMX NameNode; `HDFS_MIN_DATANODES` là số node tối thiểu mong đợi, mặc định 1 (máy tác giả có 3).

Sau bootstrap công bố báo cáo và provenance vào Neo4j, Web ở `http://localhost:3000`, API ở `http://localhost:8000/docs` (cổng có thể đổi). Dữ liệu nguồn/Silver vẫn ở HDFS/lakehouse; sau công bố, endpoint báo cáo đọc Neo4j và không cần file báo cáo cục bộ để phục vụ.

## Airflow hiện có

Hai DAG trong `portable/airflow/dags` dùng Airflow 3.3.2 và provider standard. Copy/bind mount DAG vào **Airflow đã có**, mount mã dự án tại `/opt/project`, mount `pipeline_runs`/`STATE_DIR` bền vững và cấu hình biến môi trường kết nối giống `.env`. Worker cần Java17, Spark3.5.1, Python3.11 với dependencies khóa trong `requirements-*`, và JAR Scala2.12 của `portable/jars` tại `/opt/kafka-jars`. Cấu hình `SPARK_PYTHON`, `PYTHONPATH`, `HDFS_BIN`; wrapper `portable/hdfs` dùng Hadoop client JAR có sẵn của Spark, không chứa NameNode/DataNode.

Nếu image Airflow hiện có đã đáp ứng phiên bản/runtime thì tái sử dụng. Dockerfile.airflow là lựa chọn đóng gói runtime worker khi cần, và được CI kiểm tra import; Compose production không tạo thêm Airflow. Tạo pool `spark_serial` một slot trên Airflow hiện có (`airflow pools set spark_serial 1 'Serial Spark jobs'`), tránh chạy đồng thời nhiều job Spark. Không yêu cầu SSH/WSL hoặc Docker socket trong DAG.

`football_pipeline`: preflight → snapshot nguồn → identity → Silver → kiểm tra → công bố Silver → KNN → model trận → lưu model/KNN → công bố tất cả báo cáo Neo4j. `football_kafka_analytics` đọc ba topic theo thứ tự, dùng checkpoint bền vững trên HDFS. API `/pipeline` đọc trạng thái lần chạy thật từ PLJob; không dùng danh sách PASS cố định của dashboard nguồn.

## Seed, package và kiểm chứng

Gói có nguồn, 21 bảng Silver CSV với manifest/schema/keys/checksum, DataMB, model và Kafka connector. `appearances.csv.gz` được giải nén có kiểm tra checksum bởi `infra/expand_raw.py` khi build/import. Không cần archive Hadoop hoặc image tar. Mỗi dòng dữ liệu thô được giữ trong Neo4j dưới dạng JSON array cùng header/line để không làm mất cột trùng tên.

```sh
python infra/package_portable.py
python portable/verify_bundle.py --archive artifacts/PremierLeague_Docker.zip
```

CI kiểm tra Dockerfile, DAG import, Neo4j/Kafka thật trên runner, unit / schema / tổng số / import hai lần / replay / run thiếu / mất Neo4j / restart API / Web proxy và tạo artifact ZIP + SHA256. HDFS trong CI là filesystem fixture; nghiệm thu HDFS thật phải kiểm tra read-back Parquet và cổng DataNode ở môi trường triển khai.

Ảnh ở `artifacts/screenshots` chỉ là xem trước bằng kết quả tính trực tiếp, có banner ghi rõ chưa kết nối Neo4j. Không coi ảnh này là bằng chứng Database hay CI thành công. Chưa có máy nhận nên việc chạy thực tế trên máy khác chưa được kiểm chứng; kiểm tra ZIP khi giải nén sang đường dẫn khác chỉ xác nhận cấu trúc/checksum.
