# KNN tìm đội bóng tương tự

`knn_league_table.py` dùng Spark SQL và Spark MLlib để tìm các đội/mùa có phong
độ gần nhau. Mặc định:

- Mùa truy vấn: `2024-2025`.
- Mùa tham chiếu: `2022-2023` và `2023-2024`.
- 5 láng giềng gần nhất cho mỗi đội.
- Chỉ dùng mùa lịch sử trước `2025-2026`; script sẽ từ chối mùa giả lập.

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
láng giềng từng nằm Top 4), nhóm dự đoán và danh sách các đội gần nhất.

## Chạy

Bật HDFS đủ ba DataNode trước:

```bash
cd /mnt/d/BigData/Generate_LeagueTable_Api
bash hdfs_start_3nodes.sh
```

Chạy KNN trong WSL bằng môi trường Spark của dự án:

```bash
cd /mnt/d/BigData/Generate_LeagueTable_Api/analystic/knn
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
  --output-dir outputs
```

Không cần bật Kafka, FastAPI hoặc `streaming_spark.py` cho báo cáo này. Có thể
đọc Parquet local bằng cách đổi `--input-path` thành
`/mnt/d/BigData/data_silver/parquet/facts/fact_team_season`.

## Kết quả

```text
outputs/knn_predictions.csv
outputs/knn_neighbors.csv
outputs/knn_league_table.html
```

Mở biểu đồ trên Windows:

```bash
explorer.exe 'D:\BigData\Generate_LeagueTable_Api\analystic\knn\outputs\knn_league_table.html'
```

Hoặc mở trực tiếp file
`D:\BigData\Generate_LeagueTable_Api\analystic\knn\outputs\knn_league_table.html`.

HTML có danh sách chọn đội, thanh xác suất Top 4 cho cả mùa và bảng các đội/mùa
gần nhất của đội đang chọn.

## Checkpoint lịch sử trên HDFS

Mỗi lần chạy tính SHA-256 từ các dòng dữ liệu Spark SQL đã chọn và lưu trạng
thái tại:

```text
hdfs://localhost:9000/bigdata/checkpoints/analytics/knn_league_table/latest.json
hdfs://localhost:9000/bigdata/checkpoints/analytics/knn_league_table/history.jsonl
```

Nếu đường dẫn input, danh sách mùa, mùa truy vấn, số láng giềng và fingerprint
đều giống `latest.json`, đồng thời ba file output vẫn còn, script báo:
`HDFS input has not changed; KNN outputs are already up to date.` và không chạy
lại. Vì vậy chạy lại nhiều lần không tạo bản ghi lịch sử trùng và không ghi đè
file kết quả.

Muốn chạy lại có chủ ý (ví dụ đổi code hoặc cần tái tạo HTML), thêm `--force`:

```bash
/home/linh/.venvs/league-table/bin/spark-submit \
  knn_league_table.py --force \
  --input-path hdfs://localhost:9000/bigdata/processed/facts/fact_team_season
```

Checkpoint chỉ được cập nhật sau khi cả ba file output local đã ghi thành công.
Nếu HDFS không truy cập được, job dừng và không cập nhật checkpoint; kết quả
lần chạy trước vẫn giữ nguyên.
