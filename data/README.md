# Data layout

- `bronze/fbref/` và `bronze/transfermarkt/`: bản nguồn có cấu trúc, chia theo mùa khi Airflow nhập vào HDFS.
- `silver/`: seed/fixture cục bộ; dữ liệu chuẩn hóa đầy đủ được ghi thành Parquet trên HDFS.
- `gold/`: dành cho fixture nhỏ. Báo cáo Gold đã công bố được lưu trên HDFS; Neon chỉ giữ báo cáo, provenance và trạng thái chạy.

Đường dẫn lakehouse chuẩn: `hdfs:///premier-league/{bronze,silver,gold}`.
