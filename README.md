# Premier League Analyst 2022–2025

Hệ thống phân tích Premier League, gộp pipeline chuẩn hóa, KNN và dự đoán trận với logic từ `football_analysis_share` và `Danh`. Airflow điều phối Spark trên HDFS; dữ liệu nguồn và Silver ở HDFS, còn Neon PostgreSQL chỉ lưu báo cáo phân tích, lịch sử chạy và trạng thái pipeline để API/Web truy vấn.

## 1. Kiến trúc tổng thể

```mermaid
flowchart LR
    Sources["FBref · Transfermarkt · DataMB"] --> Pipeline["Airflow điều phối Spark"]
    Pipeline --> Lake["HDFS: Bronze → Silver → Gold"]
    Lake --> Analysis["Phân tích · KNN · model trận"]
    Analysis --> Publish["Kiểm tra và công bố hoàn chỉnh"]
    Publish --> DB["Neon PostgreSQL: báo cáo · phiên bản · lịch sử"]
    Browser["Trình duyệt"] --> Web["Next.js: Web + API proxy"]
    Web --> API["FastAPI"]
    API --> DB
    API --> Kafka["Kafka: sự kiện dự đoán / mô phỏng"]
    Kafka --> Stream["Spark streaming qua Airflow"]
    Stream --> Lake
    Stream --> DB
```

| Thành phần | Vai trò | Mã chính |
| --- | --- | --- |
| Frontend | Dashboard trắng–xanh, bộ lọc, biểu đồ, dự đoán và lịch sử | `apps/web/` |
| Backend | API Neon, dự đoán trận, định giá, tương đồng và mô phỏng | `apps/football/analytics/match_outcome/api.py`, `analytics/api.py` |
| Spark | Chuẩn hóa Raw/Silver, phân tích batch, huấn luyện và xử lý sự kiện | `pipelines/spark/`, `analytics/`, `apps/football/` |
| Airflow | Điều phối pipeline và consumer Kafka, trạng thái lần chạy thực | `pipelines/airflow/dags/football.py`, `portable/runtime.py` |
| Kafka | Sự kiện dự đoán, chuyển nhượng và chuẩn hóa giải đấu | `match_outcome/stream.py`, `analytics/streaming.py` |
| Neon PostgreSQL | Báo cáo JSONB, phiên bản phân tích, bộ lọc, lịch sử chạy và trạng thái pipeline | `analytics/neon_store.py`, `analytics/publish.py` |
| HDFS | Bronze CSV, 21 bảng Silver Parquet, Gold, model và checkpoint | `pipelines/spark/pl_pipeline/`, `portable/hdfs` |

Kafka, Airflow và Hadoop là **hạ tầng ngoài đã có**; Neon là dịch vụ PostgreSQL từ xa. Compose production chỉ đóng gói API/Web/runtime. Script mã nguồn trong `scripts/` dùng `docker start` cho container Kafka/Airflow hiện có, không build/pull image, tạo container hoặc tạo Hadoop.

## 2. Dữ liệu và chức năng

- Ba mùa 2022/23–2024/25: **1.140 trận, 2.280 đội–trận, 60 đội–mùa, 240 giai đoạn**, 3.445 bàn thắng đã đối chiếu tỉ số.
- **21 bảng Silver, 73.075 dòng**; **1.350 bản ghi DataMB**. Các ID DataMB trùng được giữ cùng nguồn gốc, không gộp mất bản ghi.
- Tổng quan, bảng xếp hạng/KNN, hồ sơ và so sánh CLB, phong độ bốn giai đoạn, tiềm năng/rủi ro cầu thủ theo cửa sổ 1–3 mùa.
- Spark SQL chuyển nhượng, thu–chi, hiệu quả chi tiêu, RFM/GMM thật; nhóm theo ngưỡng được đặt tên phương pháp riêng.
- Radar/cosine cầu thủ, định giá/quỹ đạo, mô phỏng chuyển nhượng có nhãn tách dữ liệu thật.
- Dự đoán trước và trong trận, lịch sử đối đầu, trận tương đồng và yếu tố ảnh hưởng.

Schema, khóa và checksum nằm trong `data/`; nguồn gộp ghi tại [integration_manifest.json](data/integration_manifest.json). JSON giữ phân biệt `null`, chuỗi rỗng và số 0; DECIMAL được giữ bằng chuỗi chính xác.

## 3. Cấu trúc dữ liệu trên HDFS

Pipeline ghi vào `hdfs:///premier-league/`, với mùa được phân vùng `2022-23`, `2023-24`, `2024-25`. Transfermarkt giữ file nguồn nguyên vẹn dưới `_source` và tạo các tệp theo mùa để truy vấn Bronze. Silver được chia theo miền dữ liệu; mỗi bảng có thư mục `runs/<run_id>` bất biến. Gold lưu đầu ra phân tích cùng run để giữ lịch sử và chỉ Neon trỏ đến phiên bản đã công bố đầy đủ.

```text
hdfs:///premier-league/
├── bronze/
│   ├── fbref/season=2022-23|2023-24|2024-25/
│   └── transfermarkt/_source/ + season=2022-23|2023-24|2024-25/
├── silver/
│   ├── teams/       players/       matches/       transfers/
│   ├── team_season_stats/          player_season_stats/
│   └── _runs/<run_id>/_VALIDATED
├── gold/
│   ├── team_performance/          player_performance/
│   ├── season_comparison/         league_standings/
│   ├── transfer_impact/           tactical_insights/
│   └── prediction_features/
└── checkpoints/
```

Neon không nhận bản sao của các dòng Bronze/Silver. `pl_analysis_runs` lưu thời điểm, trạng thái, dataset và số báo cáo; `pl_analysis_reports` lưu payload JSONB theo loại/bộ lọc/model; `pl_analysis_active_reports` trỏ đến run hoàn chỉnh gần nhất; `pl_pipeline_jobs` lưu trạng thái từng bước Airflow. Schema được tạo bằng `CREATE TABLE IF NOT EXISTS` và index `IF NOT EXISTS`.

## 4. Cấu trúc project

```text
.
├── apps/
│   ├── web/                            # React/Next.js UI và proxy API
│   └── football/analytics/             # API trận đấu, KNN, model và streaming
├── analytics/                          # Báo cáo, API serving, Neon store
├── pipelines/
│   ├── airflow/dags/                   # DAG điều phối batch và Kafka
│   └── spark/                          # Ingest, identity, Silver, validation
├── data/
│   ├── bronze/fbref/                   # CSV nguồn FBref theo mùa
│   ├── bronze/transfermarkt/           # CSV nguồn Transfermarkt
│   ├── silver/                         # Fixture/seed đã chuẩn hóa
│   └── gold/                           # Thư mục dành cho fixture nhỏ; Gold chuẩn ở HDFS
├── portable/                           # Dockerfile, Compose, dependency, seed
├── scripts/                            # Launcher Windows/Linux cho app và dịch vụ
├── tests/                              # Kiểm thử tích hợp, API và trình duyệt
├── docs/validation/                    # Phương pháp, evidence và giới hạn
├── infra/                              # Đóng gói, manifest và mở rộng nguồn nén
└── .github/workflows/ci.yml             # Build/test, HDFS tạm, Airflow và Neon
```

## 5. Chạy frontend, backend, Kafka và Airflow

### Chuẩn bị một lần

Chạy mã nguồn bằng Python **3.11**, Node.js **>=20.9**, Docker engine hoạt động và container Kafka/Airflow đã có. Runtime phân tích đóng gói dùng Java **17**, Spark **3.5.1**, Scala **2.12**, Kafka connector tương ứng và Airflow **3.3.2**. Dependency khóa trong `portable/requirements-*.txt` và `apps/web/package-lock.json`.

Windows PowerShell, tại root repository:

```powershell
# Chỉ tạo .env nếu chưa có; giữ cấu hình và mật khẩu hiện tại.
if (-not (Test-Path portable/.env)) { Copy-Item portable/.env.example portable/.env }
py -3.11 -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -r portable/requirements-api.txt
Push-Location 'apps/web'
npm ci
Pop-Location
```

Linux/WSL:

```bash
test -f portable/.env || cp portable/.env.example portable/.env
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -r portable/requirements-api.txt
(cd 'apps/web' && npm ci)
```

Chỉnh `portable/.env` theo dịch vụ thực tế và đặt `NEON_DATABASE_URL` bằng nội dung file local `NeonKey.txt`. Cả hai file đều bị Git bỏ qua; không đưa connection string vào source hay log. `NEON_DATABASE_URL`, `KAFKA_BOOTSTRAP_SERVERS`, `HDFS_URI` dùng cho runtime/container. Launcher trên host hỗ trợ override địa chỉ dịch vụ `LOCAL_*`. Nếu không override, launcher đổi `host.docker.internal` thành `127.0.0.1`, giữ nguyên cổng và các địa chỉ khác. Kafka phải khớp **published port và advertised listeners**; `plca-kafka` trên máy tác giả dùng host port **29092**.

`KAFKA_CONTAINER` và `AIRFLOW_CONTAINERS` chọn tên container hiện có. Airflow theo thứ tự PostgreSQL → Redis → API server → scheduler → DAG processor → triggerer → worker. Không đưa container `airflow-init`/`kafka-init` vào danh sách chạy. Máy khác phải sửa tên theo executor và container của mình. Script kiểm tra toàn bộ tên trước khi khởi động, chờ healthcheck nếu có và báo lỗi nếu thiếu hoặc không sẵn sàng.

### Script Windows / Linux

| Dịch vụ | PowerShell | Bash |
| --- | --- | --- |
| Frontend | `./scripts/start-frontend.ps1` | `bash scripts/start-frontend.sh` |
| Backend | `./scripts/start-backend.ps1` | `bash scripts/start-backend.sh` |
| Kafka hiện có | `./scripts/start-kafka.ps1` | `bash scripts/start-kafka.sh` |
| Airflow hiện có | `./scripts/start-airflow.ps1` | `bash scripts/start-airflow.sh` |
| Toàn bộ | `./scripts/start-all.ps1` | `bash scripts/start-all.sh` |

Các wrapper dùng cùng `scripts/run.py`; có thể gọi trực tiếp:

```bash
python scripts/run.py all --check
python scripts/run.py all
python scripts/run.py backend --api-port 8001
python scripts/run.py frontend --api-port 8001 --web-port 3001
```

`--check` chỉ kiểm tra dependency, cấu hình, cổng và container; không tạo tiến trình, khởi động dịch vụ hay kiểm tra xác thực Database. Script chạy được từ thư mục làm việc khác; mặc định tìm `.env` theo root project. `--env-file` chọn đường dẫn tương đối root hoặc tuyệt đối; biến môi trường shell ưu tiên hơn file. `PL_PYTHON_BIN` chọn Python của venv nếu cần. Nếu PowerShell chặn script, gọi `python scripts/run.py ...` trực tiếp.

Frontend/backend chạy foreground trong terminal. **Ctrl+C** dừng các tiến trình ứng dụng do script đó tạo; container hạ tầng tiếp tục chạy. `all` dừng ứng dụng còn lại nếu một ứng dụng thoát. Script không tự cài dependency, nhập dữ liệu, huấn luyện, trigger DAG hoặc thay đổi Database. Frontend production cần `npm run build`, sau đó thêm `--production`.

Mặc định Web: **http://127.0.0.1:3000**, API docs: **http://127.0.0.1:8000/docs**. `API_PORT`/`WEB_PORT` đổi cổng; script báo lỗi khi cổng đã được dùng. Airflow UI dùng cổng container đã publish, trên máy tác giả là **http://127.0.0.1:8081**. API cần báo cáo đã công bố; `/health` trả 503 khi Neon hoặc publication chưa sẵn sàng.

### Gói Docker và DAG Airflow

[Hướng dẫn triển khai](portable/README_VI.md) mô tả Compose ứng dụng, mạng HDFS/DataNode và mount project/DAG vào Airflow hiện có. `portable/start.ps1` / `portable/start.sh` chạy **gói ứng dụng Docker**, có thể tạo container ứng dụng và chạy publication; chỉ dùng khi chọn cách triển khai đó. Build ứng dụng là lựa chọn riêng, không phải hành vi của `scripts/start-*`.

Khởi động Airflow không đồng nghĩa đã cài DAG project. Worker/DAG processor cần `pipelines/airflow/dags`, mã project tại `/opt/project`, runtime Spark/JAR, biến môi trường và pool `spark_serial` theo hướng dẫn. Script giữ nguyên mount, command và credential của container hiện có.

## 6. Pipeline, Database và replay

`football_pipeline`: kiểm tra HDFS có ít nhất ba DataNode → nhập Bronze → identity → chuẩn hóa Silver → kiểm định → KNN/model/dự đoán → ghi Gold → công bố đủ báo cáo sang Neon. HDFS dùng block size 64 MiB; mỗi lần chạy có ID riêng. Neon chỉ đổi con trỏ báo cáo sau khi các bước phân tích và ghi Gold hoàn tất.

`football_kafka_analytics`: chuẩn hóa giải đấu → dự đoán trận → mô phỏng chuyển nhượng. Checkpoint bền vững; khóa sự kiện và checksum ngăn ghi trùng. Sự kiện v2 giữ model version/mốc dữ liệu; consumer vẫn nhận v1.

Neon chỉ giữ kết quả phân tích và provenance gọn; dữ liệu thô/Silver/DataMB vẫn ở HDFS. Một giao dịch PostgreSQL ghi đủ báo cáo và cập nhật con trỏ hiện hành; run lỗi không thay báo cáo tốt trước đó. API đọc báo cáo từ Neon; Database lỗi trả 503 và Web hiển thị lỗi lưu.

## 7. API và dự đoán trận

| Endpoint | Nội dung |
| --- | --- |
| `/health`, `/docs` | Trạng thái phục vụ và tài liệu API |
| `/matches`, `/knn/*` | Dữ liệu trận và báo cáo KNN |
| `POST /predict` | API dự đoán v1 tương thích |
| `/prediction/config`, `POST /predict/v2` | Dự đoán trước/trong trận, tỉ số/phút/thống kê tùy chọn |
| `/model/metrics/v2` | Đánh giá model và độ tin cậy |
| `/dashboard`, `/outlook`, `/phases`, `/transfers`, `/rfm/{method}` | Báo cáo phân tích |
| `/players`, `/players/similar`, `POST /valuation` | Cầu thủ, tương đồng, định giá |
| `POST /transfers/simulate`, `/streaming/transfers` | Mô phỏng và sự kiện đã xử lý |
| `/reports`, `/reports/{run_id}/{kind}`, `/pipeline`, `/data/catalog`, `/data/rows` | Lịch sử, pipeline và danh mục Database |

Trước trận chỉ dùng lịch sử **trước ngày dự đoán**, tối đa ba mùa: Poisson regularized, phong độ, sân nhà/khách, đối đầu có shrinkage và cosine. Trong trận bổ sung tỉ số, phút/bù giờ; trường trống không bị coi là 0. Phút 0, tỉ số 0–0, chưa nhập thống kê trả kết quả trước trận; chọn **Đã kết thúc** trả 100% theo tỉ số. Phút 90 không tự kết thúc.

Dữ liệu đến **25/05/2025**, ngày dự đoán mặc định **26/05/2025**, không phải dữ liệu live. Trên 380 trận mùa 2024/25: trước trận accuracy **51,3%**, baseline **40,8%**, model chỉ lịch sử **50,8%**; tỉ số/phút **70,8%** trên 9 trạng thái mỗi trận. Nhận diện hòa trước trận còn yếu. Thống kê nhập so với thống kê tổng kết, chưa kiểm chứng bằng dữ liệu trực tiếp theo phút. Xem [phương pháp và giới hạn](docs/validation/FORECAST_V2.md).

## 8. Kiểm thử, CI/CD và bàn giao

```bash
export PYTHONPATH="$PWD:$PWD/apps/football/analytics"
python -m unittest discover -s tests -v
python -m unittest discover -s apps/football/analytics/match_outcome/tests -v
python -m analytics.outlook.verify_outlook
python -m analytics.outlook.verify_club_phases
python infra/package_portable.py
python portable/verify_bundle.py --archive artifacts/PremierLeague_Docker.zip
```

GitHub [Actions](https://github.com/DuyLinh0212/Premier-League-Analyst-22-25/actions) kiểm tra Compose, huấn luyện theo thời gian, build API/Web/runtime, unit, DAG, rồi trên push `main` dựng HDFS tạm với một NameNode và ba DataNode trên runner, chạy pipeline Airflow đầy đủ và xác minh Bronze/Silver/Gold cùng 23 báo cáo Neon. Workflow cần repository secret `NEON_DATABASE_URL`; `NeonKey.txt` không được commit. Artifact của kiểm thử đóng gói có ZIP/SHA256; flow HDFS/Neon có artifact bằng chứng riêng.

ZIP gồm mã nguồn, script chạy, seed, model, dependency lock và manifest checksum; loại secret, cache, image tar và archive Hadoop. Đóng gói tìm root theo vị trí script, không tạo `delivery/delivery`. `appearances.csv.gz` được giải nén và kiểm tra checksum khi import/build.

CI tạo Hadoop chỉ trên runner tạm; máy laptop và máy nhận tiếp tục dùng HDFS hiện có, không format hay dựng Hadoop mới. Luồng Neon chỉ chạy trên push sau khi secret đã được cấu hình. Kiểm tra local trước đây và giới hạn triển khai ghi ở [docs/validation/README.md](docs/validation/README.md); chuyển máy thực tế vẫn cần nghiệm thu riêng.

## 9. Tài liệu liên quan

- [Triển khai với hạ tầng ngoài](portable/README_VI.md)
- [Dự đoán trước và trong trận](docs/validation/FORECAST_V2.md)
- [Dữ liệu Silver](data/silver/README_VI.md)
- [Công cụ đóng gói](infra/README.md)
- [Evidence và giới hạn](docs/validation/README.md)

Giấy phép: [LICENSE](LICENSE).
