# Premier League Analytics

Dashboard React tiếng Việt cho 10 phân tích Premier League. API truy vấn PostgreSQL
theo snapshot đã được DAG Airflow đối soát với Spark/HDFS; không khởi tạo Spark khi
mở Web. Hướng dẫn đầy đủ: [pipeline Airflow](../../src/pipeline/README_VI.md).

## Chạy nhanh trên Windows + WSL

Điều kiện:

- WSL distro tên Ubuntu.
- Python environment ~/.venvs/plca-spark đã có PySpark và Java 17 khả dụng.
- Node.js và npm trên Windows.

Mở PowerShell tại thư mục gốc repo và chạy:

    .\scripts\run_pipeline.ps1 -Action run -RunId lich_su_001
    .\scripts\start_web.ps1

Thiết lập PostgreSQL và Airflow theo README pipeline trước lần chạy đầu. Launcher
build React, chạy scheduler thật, xuất báo cáo và công bố snapshot PostgreSQL. Sau
đó `start_web.ps1` khởi động dịch vụ và mở http://127.0.0.1:5173.

Chỉ chạy phân tích Spark SQL và xuất lại report, không khởi động web:

    .\scripts\export_reports.ps1

Chọn mùa hoặc bắt buộc vẽ lại report dù đã có cache:

    .\scripts\export_reports.ps1 -Seasons 2024-2025
    .\scripts\export_reports.ps1 -Seasons @('2022-2023', '2023-2024') -Force

Script đọc 21 bảng từ Silver, chạy bốn truy vấn báo cáo tĩnh và lưu PNG cùng
JSON vào evidence/<season>/. Mặc định xử lý mọi mùa khai báo trong
silver_manifest.json.

Nếu cần cài Python dependencies thủ công trong Ubuntu/WSL:

    cd /mnt/f/NgDuyLinh/Do_an/BigData/Premier-League-Analyst-22-25/apps/analytics-web/api
    ~/.venvs/plca-spark/bin/python -m pip install -r requirements.txt

## Cấu trúc

- apps/analytics-web/api/app/queries/: đúng 10 module Spark SQL, mỗi module ứng với một tab.
- src/pipeline/database.py: truy vấn PostgreSQL, quản lý snapshot và công bố nguyên tử.
- apps/analytics-web/api/app/main.py: FastAPI, metadata, truy vấn động và báo cáo tĩnh.
- artifacts/<date>/<run_id>/reports/: PNG/JSON đã xuất cùng snapshot.
- apps/analytics-web/web/: React, Vite và Apache ECharts.
- scripts/start_web.ps1: khởi chạy cả hai dịch vụ trên máy local.
- scripts/export_reports.py / scripts/export_reports.ps1: chạy riêng Spark SQL và xuất report vào evidence/.

## Quy tắc dữ liệu

- NULL được giữ nguyên từ Silver Parquet qua PostgreSQL; không dùng CSV cũ thay thế.
- Vòng đấu nối bằng tm_game_id với games.csv của giải GB1.
- So sánh cầu thủ dùng cùng mùa, cùng vị trí và tối thiểu 900 phút mặc định.
- Radar sức mạnh xếp percentile trên toàn bộ CLB trong mùa trước khi áp dụng bộ lọc hiển thị.
- Đối đầu và diễn biến điểm có thể gộp tối đa ba mùa; đường điểm được tính độc lập theo vòng của từng mùa.
- Giá trị thị trường thiếu vẫn là NULL. Phí chuyển nhượng chỉ tính khi được báo
  cáo dương; Spearman đi kèm cỡ mẫu và không hàm ý nhân quả.
- Scatter dùng bàn thắng/trận và bàn thua/trận vì Silver không có xG.

## API

- GET /health
- GET /api/metadata?season=2024-2025&position=FW&min_minutes=900
- GET /api/player-radar?season=...&position=...&players=player_id%7Cclub_id&players=...
- GET /api/player-contribution?season=...&club_id=...
- GET /api/squad-value?season=...&club_id=... (club_id có thể bỏ để xem toàn giải)
- GET /api/matchday-performance?season=...&club_id=...
- GET /api/points-progression?club_id=...&seasons=2022-2023&seasons=2023-2024&seasons=2024-2025
- GET /api/head-to-head?club_id=...&seasons=2022-2023&seasons=2023-2024&seasons=2024-2025
- GET /api/team-strength?season=...&clubs=...
- GET /api/reports/{age-distribution|head-to-head|attack-defense|spending-vs-achievement}?season=...

API dùng cổng 8000; Web build dùng cổng 5173. Địa chỉ và đường dẫn nằm trong
config/pipeline.yaml. Mỗi phản hồi có meta.run_id và meta.source=postgresql;
PostgreSQL mất kết nối trả 503. Các script export_reports cũ là công cụ thủ công
đọc CSV; workflow Airflow không sử dụng chúng.
