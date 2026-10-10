# Premier League Analyst 2022–2025

`delivery` gộp pipeline, KNN và model thắng–hòa–thua với tiềm năng cầu thủ / bốn giai đoạn CLB của `football_analysis_share` và các chức năng có mã thực thi của `Danh`. Hai thư mục nguồn bên ngoài không bị sửa. `data/integration_manifest.json` ghi nguồn và checksum; bảy bảng trùng khớp hoàn toàn.

Web Next.js dùng API FastAPI truy vấn Neo4j. Dữ liệu nguồn, 21 bảng Silver (73.075 dòng), 1.350 bản ghi DataMB, audit, đầu vào và kết quả phân tích được lưu dưới namespace cấu hình. Không gộp 13 ID DataMB trùng qua các CLB bằng cách làm mất bản ghi. JSON giữ phân biệt `null`, chuỗi rỗng và số 0; DECIMAL lưu chuỗi chính xác. HDFS giữ lakehouse và artifact model.

Dự đoán trận có hai chế độ: trước trận dùng lịch sử trước ngày yêu cầu, đối đầu tối đa ba mùa và cosine; trong trận dùng tỉ số, phút/bù giờ và thống kê nhập để tìm trận tương đồng. API `/predict/v2` lưu toàn bộ đầu vào/kết quả vào Neo4j, Kafka replay đúng phiên bản; `/predict` cũ vẫn tương thích. Xem [phương pháp, giới hạn và kiểm thử](validation/FORECAST_V2.md). Thống kê nhập được so với thống kê tổng kết; không được quảng bá như dữ liệu trực tiếp theo phút.

Các báo cáo gồm outlook cửa sổ 1–3 mùa, 60 đội–mùa / 240 giai đoạn, bảy truy vấn Spark SQL, thu–chi / chi phí trên điểm, RFM với GMM Spark MLlib thật (k=4, seed=42), phân nhóm theo ngưỡng mang tên riêng, dashboard tổng hợp / hồ sơ CLB / bàn thắng theo 15 phút, DataMB radar / cosine similarity, định giá và quỹ đạo theo công thức của Danh. Các tính năng chỉ được nhắc trong mô tả nguồn nhưng không có mã thực thi không được coi là đã có. Dữ liệu mô phỏng có nhãn riêng và không trộn vào mùa thật.

Mỗi báo cáo có `run_id`, phiên bản logic, bộ lọc, thời điểm và trạng thái. Pointer công bố đổi trong cùng transaction với trạng thái hoàn tất, sau khi đủ báo cáo. Run lỗi / thiếu không thay thế bản tốt. Import dùng khóa và checksum; Kafka event có ID ổn định và checkpoint; ghi HDFS trước Neo4j và replay không ghi thêm bản trùng. Khi Database lỗi, API trả lỗi 503 và Web hiển thị lỗi, không báo lưu thành công.

## Chạy với hạ tầng hiện có

Đọc [hướng dẫn Windows/Linux](portable/README_VI.md). Compose production chỉ có ứng dụng API, Web, runtime và lệnh publish. Nó không tạo Hadoop, Kafka, Neo4j, Airflow hay PostgreSQL. Dockerfile được dùng để build phần ứng dụng khi máy nhận chưa có image tương thích; không cần build lại hạ tầng. Kiểm thử local của đợt gộp dùng thư mục tạm trong container Spark/Airflow/Kafka có sẵn, không build/pull hay tạo container mới.

GitHub Actions là môi trường riêng được phép dựng Neo4j và Kafka tạm. Workflow build Dockerfile, kiểm thử unit / import DAG Airflow / API / Web proxy / idempotent import / replay / sự cố Database, rồi xuất ZIP + SHA256. CI dùng fixture Silver và filesystem local cho HDFS; không thay thế nghiệm thu HDFS thật. Xem kết quả workflow của đúng commit tại Actions của repository.

## Mã nguồn và kiểm tra

`analytics/` chứa logic tích hợp và serving Neo4j; `jobs/pl_pipeline/` giữ pipeline Silver; `Generate_LeagueTable_Api/analystic/` giữ model / KNN; `Premier Leauge Club Analyst/web-next/` là Web chung. DAG tại `portable/airflow/dags` chạy chuẩn hóa → kiểm tra → phân tích → công bố; trạng thái task lưu `PLJob`, API `/pipeline` đọc lần chạy thật.

```sh
export PYTHONPATH="$PWD:$PWD/Generate_LeagueTable_Api/analystic"
python -m unittest discover -s tests -v
python -m unittest discover -s Generate_LeagueTable_Api/analystic/match_outcome/tests -v
python -m analytics.outlook.verify_outlook
python -m analytics.outlook.verify_club_phases
python infra/package_portable.py
python portable/verify_bundle.py --archive artifacts/PremierLeague_Docker.zip
```

Gói không chứa secret, cache, image tar, wheel cache hay archive Hadoop. CSV appearances lớn được chuyển sang gzip trong `data/raw` và giải nén bằng checksum khi import/build; mọi dòng nguồn vẫn được nhập Database. Không cần Git LFS. Script chạy từ `delivery`, không sinh `delivery/delivery`.
