# Premier League Analytics 2022–2025

Nền tảng phân tích Premier League hợp nhất dữ liệu FBref và Transfermarkt, chuẩn hóa bằng Spark, lưu lakehouse trên HDFS, chạy phân tích theo Airflow và công bố **báo cáo** lên Neon PostgreSQL. Dữ liệu nguồn và các bảng Silver vẫn ở HDFS; Neon chỉ giữ kết quả đã hoàn chỉnh, lịch sử chạy và trạng thái từng bước. Web Next.js đọc báo cáo qua FastAPI.

## Kiến trúc mã nguồn

```text
.
├── apps/
│   ├── web/
│   │   ├── src/app/                 # Next.js routes, API proxy, layout
│   │   ├── src/modules/dashboard/   # Các màn hình phân tích React
│   │   └── package.json
│   ├── api/
│   │   ├── analytics/               # FastAPI, báo cáo, Neon, API truy vấn
│   │   └── football/                # API và xử lý sự kiện bóng đá
├── data/
│   ├── raw/                         # FBref, Transfermarkt, DataMB nguyên bản
│   ├── lake/silver/                 # Snapshot Silver nhỏ dùng làm fixture
│   ├── samples/pipeline-seed/       # Seed CI đã kiểm tra checksum
│   └── models/                      # Model và báo cáo model đã huấn luyện
├── ingestion/hdfs/                  # Đăng ký nguồn và nhập Bronze vào HDFS
├── processing/spark/                # Chuẩn hóa, identity, kiểm định, Spark pipeline
├── ml/inference/                    # KNN và dự đoán thắng–hòa–thua
├── orchestration/dags/              # DAG batch và Kafka của Airflow
├── database/schemas/analytics.sql   # CREATE TABLE/INDEX IF NOT EXISTS cho Neon
├── infrastructure/
│   ├── docker/                      # Dockerfile, Compose CI, runtime và kiểm tra
│   └── scripts/                     # Đóng gói, bảo trì và launcher local
├── tests/                           # integration, ml, processing, orchestration
├── docs/                            # Kiến trúc, phương pháp và bằng chứng kiểm thử
├── compose.yml                      # Ứng dụng kết nối hạ tầng ngoài
├── compose.dev.yml                  # Fixture CI: API/Web, Kafka và PostgreSQL tạm tương thích Neon
├── compose.bigdata.yml              # HDFS/Airflow tạm chỉ dành cho CI
└── README.md
```

API/backend là Python FastAPI nên schema Neon được quản lý bằng SQL và `psycopg`; project không dùng Prisma. Thư mục chỉ được tạo khi có mã hoặc dữ liệu thực, không thêm các module rỗng để làm cây trông giống mẫu.

## Luồng dữ liệu

```text
FBref + Transfermarkt → ingestion → HDFS Bronze → Spark chuẩn hóa → HDFS Silver
                                                        ↓
                         KNN / outlook / phases / transfers / WDL / model
                                                        ↓
                                      HDFS Gold → công bố nguyên tử lên Neon
                                                        ↓
                                           FastAPI → Next.js dashboard
```

Airflow DAG `football_pipeline` chạy preflight HDFS, nạp nguồn, tạo identity, chuẩn hóa và kiểm định Silver, huấn luyện/phân tích, ghi Gold rồi mới công bố các báo cáo hoàn chỉnh. DAG Kafka dùng checkpoint và khóa sự kiện để hỗ trợ replay. HDFS giữ dữ liệu lớn; Neon chỉ lưu báo cáo, phiên bản, bộ lọc, metadata nguồn và trạng thái pipeline. `database/schemas/analytics.sql` là schema idempotent mà API/pipeline tự áp dụng.

Dự đoán trận hỗ trợ trước trận và trong trận, dùng dữ liệu tối đa ba mùa và không đưa thống kê tương lai vào đặc trưng. Mốc dữ liệu hiện có đến 25/05/2025; đây không phải nguồn live. Xem [phương pháp và giới hạn](docs/validation/FORECAST_V2.md).

Web/backend chỉ cần Neon có các báo cáo và model đã công bố để đọc và dự đoán; không cần khởi chạy HDFS, Spark hoặc Airflow cho từng lần dự đoán. Các thành phần đó chỉ cần khi nhập/chuẩn hóa dữ liệu hay huấn luyện và công bố phiên bản phân tích mới. Kafka là luồng bổ sung: kết quả dự đoán vẫn được lưu và trả về khi Kafka tắt. Neon giữ báo cáo; danh mục và phân trang dữ liệu trên Web đọc từ các báo cáo Neon đã công bố, còn bảng nguồn lớn vẫn nằm ở HDFS.

## Chạy ứng dụng trên máy cá nhân

Cài Python 3.11, Node.js từ 20.9 và cấu hình các dịch vụ hiện có trong `.env` dựa theo [.env.example](.env.example). File `NeonKey.txt` chỉ là nguồn bí mật cục bộ, không được commit. Không có lệnh nào trong launcher tự dựng Hadoop, Kafka hay Airflow.

```powershell
py -3.11 -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -r infrastructure/docker/requirements-api.txt
Push-Location apps/web
npm ci
Pop-Location
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
```

Trên Linux/WSL, dùng `python3.11 -m venv .venv`, kích hoạt `.venv`, rồi cài cùng file requirements và chạy `npm ci` trong `apps/web`.

| Chạy | Windows PowerShell | Linux/WSL |
| --- | --- | --- |
| Frontend | `./infrastructure/scripts/launchers/start-frontend.ps1` | `bash infrastructure/scripts/launchers/start-frontend.sh` |
| Backend | `./infrastructure/scripts/launchers/start-backend.ps1` | `bash infrastructure/scripts/launchers/start-backend.sh` |
| Backend + Web only | `./infrastructure/scripts/launchers/start-web-backend.ps1` | `bash infrastructure/scripts/launchers/start-web-backend.sh` |
| Kafka hiện có | `./infrastructure/scripts/launchers/start-kafka.ps1` | `bash infrastructure/scripts/launchers/start-kafka.sh` |
| Airflow hiện có | `./infrastructure/scripts/launchers/start-airflow.ps1` | `bash infrastructure/scripts/launchers/start-airflow.sh` |
| Tất cả | `./infrastructure/scripts/launchers/start-all.ps1` | `bash infrastructure/scripts/launchers/start-all.sh` |

Launcher đọc `.env` ở root, kiểm tra tên và health của container hiện có rồi dùng `docker start`; nó không tạo hoặc build container. Lệnh Backend + Web chỉ chạy hai ứng dụng và không khởi động Kafka/Airflow. `python infrastructure/scripts/launchers/run.py all --check` chỉ preflight. Mặc định Web ở `http://127.0.0.1:3000`, API docs ở `http://127.0.0.1:8000/docs`.

## Đóng gói Docker và CI

`compose.yml` đóng gói API/Web/runtime để kết nối Kafka, Airflow, HDFS và Neon bên ngoài. Cấu hình image ứng dụng tương thích trong `.env`; khi máy nhận chưa có image ứng dụng thì build từ `infrastructure/docker/Dockerfile.*`. Hadoop client dùng thư viện Hadoop kèm Spark; Dockerfile không cài NameNode/DataNode.

`compose.dev.yml` và `compose.bigdata.yml` phục vụ GitHub Actions. Runner CI mới dựng dịch vụ tạm (gồm ba DataNode), chạy Airflow, kiểm tra HDFS và công bố báo cáo lên Neon. Các container Hadoop trong `compose.bigdata.yml` chỉ chạy trong runner GitHub, không phải Compose laptop.

Tạo gói bàn giao cùng checksum:

```bash
python infrastructure/scripts/build/package_portable.py
python infrastructure/docker/verify_bundle.py --archive artifacts/PremierLeague_Docker.zip
```

Gói bỏ secret, cache, image tar và Hadoop archive. GitHub Actions yêu cầu repository secret `NEON_DATABASE_URL` để chạy pipeline HDFS/Airflow và xác minh báo cáo Neon; artifact bằng chứng được đính kèm cùng workflow.

## Kiểm thử

```bash
# PowerShell
$env:PYTHONPATH = "apps/api;ml/inference;processing/spark"
python -m unittest discover -s tests -v
python -m unittest discover -s apps/api/football/tests -v
python -m analytics.outlook.verify_outlook
python -m analytics.outlook.verify_club_phases
```

`npm run build` trong `apps/web` xác nhận Next.js. CI còn build các image ứng dụng, import DAG Airflow, chạy fixture Kafka/PostgreSQL tương thích Neon, test Web khi bỏ các file báo cáo/model cục bộ, rồi chạy pipeline HDFS và Neon trên runner riêng.
