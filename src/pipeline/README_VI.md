# Airflow → HDFS → PostgreSQL → Web, không Docker

Pipeline lịch sử xử lý **586 file nguồn**, ba mùa **2022–2023, 2023–2024, 2024–2025**.
Kafka mô phỏng trong `src/ingest` chạy độc lập. Pipeline này không dùng Machine Learning.

## Chạy trên máy hiện tại

Trong PowerShell Administrator, thiết lập PostgreSQL một lần:

```powershell
.\scripts\setup_pipeline_postgres.ps1
```

Mật khẩu quản trị được nhập kín, chỉ dùng trong tiến trình setup. Script tạo database
`premier_league_airflow`, `premier_league_analytics` và ba tài khoản ứng dụng riêng.
Chạy lại không thay mật khẩu tài khoản đã có. Mật khẩu ứng dụng lưu trong WSL ở
`~/.config/plca/credentials.json`, quyền `0600`; không đưa file này vào Git/artifact.
Script dùng SCRAM và chỉ mở firewall cho subnet WSL hiện tại. Sau khi WSL đổi subnet,
chạy lại script setup để cập nhật luật kết nối. Không dùng `trust` hoặc mở mọi IP.

Thiết lập thư viện và Airflow trên môi trường đã cài, sau đó chạy đầy đủ:

```powershell
.\scripts\run_pipeline.ps1 -Action setup -SkipPostgresSetup
.\scripts\run_pipeline.ps1 -Action dry-run
.\scripts\run_pipeline.ps1 -Action run -RunId lich_su_2022_2025_001
.\scripts\run_pipeline.ps1 -Action status
```

`run` kiểm thử, build Web, khởi động dịch vụ cần thiết, kích hoạt DAG qua REST API,
chờ scheduler thực thi, đối soát PostgreSQL, kiểm tra trình duyệt và thu artifact.
Không sử dụng `airflow dags test`. Cùng `RunId` giữ nguyên snapshot nguồn/cấu hình;
sau khi sửa nguyên nhân lỗi, chạy lại để tiếp tục các task lỗi. Đổi nguồn hoặc cấu
hình cần `RunId` mới. Retry không tạo bản trùng cho lần chạy đã hoàn tất.

```powershell
.\scripts\run_pipeline.ps1 -Action start
.\scripts\run_pipeline.ps1 -Action stop
```

`start` chạy Airflow và mở dịch vụ API/Web nếu đã có snapshot công bố. `stop` dừng
Airflow/API/Web do launcher quản lý; từ chối ngắt một DAG đang chạy. Dữ liệu, HDFS,
PostgreSQL và Kafka độc lập được giữ nguyên. Web: http://127.0.0.1:5173;
API: http://127.0.0.1:8000/docs; Airflow: http://127.0.0.1:8080.
Tài khoản Airflow `plca`; mật khẩu riêng nằm trong file `service-secrets.json` dưới
thư mục state. File này có quyền `0600` và luôn bị loại khỏi artifact.

## Cấu hình và dữ liệu

`config/pipeline.yaml` quản lý nguồn, ba mùa, HDFS, các môi trường Python, URL, giới
hạn phút thi đấu, CLB minh họa và artifact. `PLCA_PIPELINE_CONFIG` cho phép chọn file
cấu hình khác; `PLCA_PIPELINE_STATE`, `PLCA_CREDENTIALS_FILE`, `PLCA_POSTGRES_HOST`
cho phép ghi đè môi trường. Mặc định địa chỉ Windows được dò từ route WSL.

Runtime nằm trên filesystem Linux: `~/.local/share/premier-league-pipeline/`.
Airflow home mới `~/.local/share/premier-league-airflow/` dùng PostgreSQL metadata;
giữ nguyên Airflow/SQLite cũ trong `~/airflow`. Dùng Airflow 3.3.2, Spark 4.1.0,
Java 17, PostgreSQL 18 ở `D:\PostgreSQL\18`; không cài lại Spark hoặc Airflow.
Các phiên bản Python/PostgreSQL này nằm trong [prerequisites Airflow 3.3.2](https://airflow.apache.org/docs/apache-airflow/3.3.2/installation/prerequisites.html).

| Tầng | Đường dẫn HDFS |
|---|---|
| Bronze và SHA-256 nguồn | `/PremierLeagueAnalysis/bronze/snapshots/<run_id>/` |
| Nguồn chuẩn hóa Parquet, schema và bản đồ cột | `/PremierLeagueAnalysis/silver/runs/<run_id>/normalized_sources/` |
| 21 bảng Silver và `tm_games` | `/PremierLeagueAnalysis/silver/runs/<run_id>/tables/` |
| Parquet phân tích, báo cáo, Excel, chi tiết | `/PremierLeagueAnalysis/golde/reports/report_date=YYYY-MM-DD/run_id=<run_id>/` |
| Minh chứng từng lần thử, kể cả thất bại | `/PremierLeagueAnalysis/golde/reports/report_date=YYYY-MM-DD/evidence/run_id=<run_id>/attempt=.../` |

Ngày báo cáo theo giờ Việt Nam, được chốt lúc tạo run. Bronze, Silver và Gold ghi
qua staging. Chỉ đọc Gold có `_SUCCESS`. HDFS lỗi làm task thất bại; không ghi thay
sang local hoặc dùng Silver CSV cũ. Toàn bộ bảng Silver được dựng lại từ **Parquet
chuẩn hóa của chính snapshot Bronze**, với quy tắc định danh FBref/Transfermarkt.

Giữ NULL khi thiếu tuổi, định giá, tiền lương hoặc không ghép được nguồn. Có bảng
`identity_review`; `points_adjustment` lưu chênh lệch điểm chính thức và điểm thắng/hòa.
Không ép điểm kỷ luật khớp công thức thi đấu. Radar dùng percentile cùng cohort trước
khi lọc lựa chọn; độ lệch chuẩn được làm tròn 12 chữ số để các giá trị đồng hạng không
bị phân tách bởi sai số dấu phẩy động giữa Spark và PostgreSQL.

PostgreSQL lưu 22 bảng, kết quả phân tích JSONB và manifest trong schema riêng theo
`run_id`. Giao dịch chỉ chuyển `plca.active.run_id` khi nạp/đối chiếu xong. API dùng
tài khoản `plca_reader`, giao dịch chỉ đọc với cùng snapshot trong mỗi request.
`/health` và `meta` trả `source=postgresql`, `run_id`; Web không import hoặc khởi động
Spark. Báo cáo PNG đọc theo cùng run. Khi kiểm tra Web thất bại sau công bố, phục hồi
snapshot cũ; nếu DB đang mất kết nối, ghi yêu cầu phục hồi và xử lý trước lần start sau.

## DAG và nghiệm thu

DAG `premier_league_reports` không có lịch tự chạy, `max_active_runs=1`, pool
`plca_spark` có một slot. Task retry hai lần, có timeout, log tiếng Việt; XCom không
chứa dữ liệu. Các bước: kiểm tra → Bronze → chuẩn hóa → định danh → Silver → kiểm
tra Silver → 10 phân tích → xuất báo cáo → lưu HDFS → nạp PostgreSQL → đối chiếu → công bố.

Kiểm thử nhanh:

```powershell
.\src\pipeline\tests\check_setup_bridge.ps1
wsl -d Ubuntu --cd /mnt/f/NgDuyLinh/Do_an/BigData/Premier-League-Analyst-22-25 --exec /root/.venvs/plca-spark/bin/python -m pytest src/pipeline/tests -q
```

Kiểm thử gồm import DAG trên Airflow cài thật, PowerShell 5.1/7 và UTF-8 BOM, đọc/ghi
JSON qua WSL, tính bất biến run, NULL, chuyển SQL, che bí mật và API không import
Spark. DAG đối chiếu checksum toàn bộ nguồn trên HDFS; số trận, khóa, liên kết,
tỷ số; đọc lại Parquet và so sánh toàn bộ kết quả cả 10 phân tích với PostgreSQL.

Artifact local: `artifacts/YYYY-MM-DD/<run_id>/`. Có log Việt/technical, trạng thái
DAG/task, ảnh terminal đọc log thực, ảnh Airflow DAG/run/task log, 30 ảnh Web
(10 tab × 3 mùa), phản hồi mạng chứng minh nguồn PostgreSQL/run, 30 PNG báo cáo,
JSON, chi tiết 22 bảng, Excel 10 sheet, SVG kiến trúc/DAG và SHA-256 mọi file.
Excel `so_sanh_3_mua.xlsx` có so sánh cả ba cặp mùa bằng công thức, ô trống cho đội
không tham gia; dữ liệu chi tiết bao phủ tất cả đội/cầu thủ hợp lệ. Radar xuất
cohort từng cầu thủ, không tạo mọi tổ hợp chọn 2–4 người. Minh họa dùng Arsenal,
bốn đội ARS/MCI/LIV/CHE và hai tiền đạo đủ 900 phút chơi nhiều nhất của mùa.

## GitHub Actions trên máy này

```powershell
.\scripts\setup_pipeline_runner.ps1 -Action setup
.\scripts\setup_pipeline_runner.ps1 -Action status
.\scripts\setup_pipeline_runner.ps1 -Action start
```

Setup nhận registration token kín từ GitHub Settings → Actions → Runners;
`-UseGitCredentialManager` dùng phiên GitHub đã đăng nhập để xin token tạm qua API.
Token không nằm trong Git. Runner chạy dưới tài khoản Windows hiện tại để truy cập
đúng Ubuntu/WSL, nhãn `plca-local`. Sau khi khởi động lại Windows, dùng `-Action start`.
Xem [hướng dẫn đăng ký runner của GitHub](https://docs.github.com/en/actions/how-tos/manage-runners/self-hosted-runners/add-runners).

Workflow `.github/workflows/ci.yml` chạy khi push `main` hoặc **Run workflow**.
Checkout riêng trong thư mục runner, chỉ sao chép 586 nguồn từ dự án gốc; không reset,
clean hoặc xóa thư mục làm việc hiện tại. Chỉ một job dùng máy tại một thời điểm;
không hủy khi đang công bố. Log/artifact luôn được gom khi lỗi, tải GitHub giữ 30
ngày và lưu HDFS theo ngày. Máy và runner cần hoạt động trong suốt job.
