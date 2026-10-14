# KNN dự đoán bảng xếp hạng

`knn_league_table.py` dùng Spark SQL và Spark MLlib để ước tính thứ hạng, điểm
số và khả năng vào Top 4 của các đội trong mùa được chọn. Airflow cấu hình mùa
truy vấn là `2025-2026`, dùng ba mùa 2022–2025 làm dữ liệu tham chiếu.

- Mùa truy vấn của Airflow: `2025-2026`.
- Mùa tham chiếu: `2022-2023`, `2023-2024` và `2024-2025`.
- 5 đội tương đồng gần nhất được dùng nội bộ cho mỗi đội.
- Dữ liệu mùa 2025–2026 phải được ghi vào HDFS bởi luồng Kafka trước bước KNN.
  Nếu thiếu phân vùng mùa này, Airflow báo thiếu dữ liệu thay vì hiển thị nhầm
  dự đoán 2024–2025.

## Đặc trưng và chuẩn hóa

Spark SQL tính năm đặc trưng cho mỗi đội/mùa:

```text
points_per_mp
goal_diff_per_match
goals_for_per_match
goals_against_per_match
wins_per_match
```

Sau đó `StandardScaler(withMean=True, withStd=True)` chuẩn hóa toàn bộ đặc
trưng trước khi tính khoảng cách Euclid và chọn K láng giềng. Với mỗi đội của
mùa truy vấn, báo cáo đưa ra hạng/điểm dự đoán trung bình, xác suất Top 4 (tỷ lệ
láng giềng từng nằm Top 4) và nhóm dự đoán. Các láng giềng chỉ là đầu vào của
phép ước tính, không được xuất thành chức năng tra cứu đội tương tự.

## Chạy

Bật HDFS đủ ba DataNode trước:

```bash
cd "$PROJECT_ROOT"
bash hdfs_start_3nodes.sh
```

Chạy KNN trong WSL bằng môi trường Spark của dự án:

```bash
cd "$PROJECT_ROOT/ml/inference/knn"
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export SPARK_LOCAL_IP=127.0.0.1
export PYSPARK_PYTHON=/home/linh/.venvs/league-table/bin/python
export PYSPARK_DRIVER_PYTHON=/home/linh/.venvs/league-table/bin/python

/home/linh/.venvs/league-table/bin/spark-submit \
  knn_league_table.py \
  --input-path hdfs://localhost:9000/bigdata/processed/facts/fact_team_season \
  --seasons 2022-2023,2023-2024,2024-2025 \
  --query-season 2024-2025 \
  --neighbors 5 \
  --output-dir ../../data/models/knn
```

Không cần bật Kafka, FastAPI hoặc `streaming_spark.py` cho báo cáo này. Có thể
đọc Parquet local bằng cách đổi `--input-path` thành
`/mnt/d/BigData/data_silver/parquet/facts/fact_team_season`.

## Kết quả

`data/models/knn/knn_predictions.csv` chứa `actual_rank`, `actual_points` (từ
bảng mô phỏng Kafka) cùng `predicted_rank`, `predicted_points` và
`top4_probability` để đối chiếu.

Lưu ý: luồng Kafka hiện tạo bảng mô phỏng đầy đủ cả mùa. KNN dùng thống kê cả
mùa đó để ước tính thứ hạng; đây là minh họa phân tích trên dữ liệu mô phỏng,
không phải dự báo trước mùa giải hoặc kết quả thực tế của Premier League.

## Checkpoint lịch sử trên HDFS

Mỗi lần chạy tính SHA-256 từ các dòng dữ liệu Spark SQL đã chọn và lưu trạng
thái tại:

```text
hdfs://localhost:9000/bigdata/checkpoints/analytics/knn_league_table/latest.json
hdfs://localhost:9000/bigdata/checkpoints/analytics/knn_league_table/history.jsonl
```

Nếu đường dẫn input, danh sách mùa, mùa truy vấn, số láng giềng, fingerprint và
định dạng output đều giống `latest.json`, đồng thời file dự đoán vẫn còn, script báo:
`HDFS input has not changed; KNN outputs are already up to date.` và không chạy
lại. Vì vậy chạy lại nhiều lần không tạo bản ghi lịch sử trùng và không ghi đè
file kết quả.

Muốn chạy lại có chủ ý (ví dụ đổi code), thêm `--force`:

```bash
/home/linh/.venvs/league-table/bin/spark-submit \
  knn_league_table.py --force \
  --input-path hdfs://localhost:9000/bigdata/processed/facts/fact_team_season
```

Checkpoint chỉ được cập nhật sau khi file output local đã ghi thành công.
Nếu HDFS không truy cập được, job dừng và không cập nhật checkpoint; kết quả
lần chạy trước vẫn giữ nguyên.
