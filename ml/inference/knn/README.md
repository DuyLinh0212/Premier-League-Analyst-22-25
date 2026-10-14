# KNN xếp hạng đội bóng theo bốn chỉ số

Trang web nhận bốn giá trị do người dùng nhập cho mỗi đội:

| Tên trên trang | Đặc trưng trong mô hình |
| --- | --- |
| PPG — điểm trung bình mỗi trận | `points_per_mp` |
| Goal Difference/Game — hiệu số trung bình mỗi trận | `goal_diff_per_match` |
| Recent Form — điểm trung bình 5 trận gần nhất | `recent_form` |
| Goals Scored/Game — bàn thắng trung bình mỗi trận | `goals_for_per_match` |

Mô hình dùng các đội/mùa có kết quả thật trong 2022–2023, 2023–2024 và
2024–2025 làm mẫu tham chiếu. Tập lịch sử được lấy từ Silver: bảng tổng kết
đội/mùa và kết quả từng trận. Recent Form lịch sử là trung bình điểm của năm
trận cuối mùa (thắng 3, hòa 1, thua 0).

## Cách KNN tính hạng

1. Tính Min và Max của từng đặc trưng trên tập tham chiếu.
2. Chuẩn hóa cả mẫu lịch sử và dữ liệu người dùng về [0, 1]:
   `(giá trị - Min) / (Max - Min)`. Nếu Min bằng Max, giá trị chuẩn hóa bằng 0.
3. Tính khoảng cách Euclid trên bốn đặc trưng và chọn 5 láng giềng gần nhất.
4. Lấy trung bình hạng thật của năm láng giềng làm điểm xếp hạng tương đối;
   sắp xếp các đội người dùng nhập theo điểm này để xuất hạng KNN.

## Metric report

Airflow tính kiểm định hồi cứu theo mùa. Mỗi lượt kiểm định giữ lại một mùa,
chỉ dùng các mùa trước đó để huấn luyện và khớp MinMax. Báo cáo gồm Accuracy,
Precision, Recall, F1 của nhóm Top 4, MAE/RMSE thứ hạng, tương quan thứ hạng,
số đội và kết quả theo từng mùa. Đây là đánh giá trên mùa đã hoàn tất, không
phải kết quả dự báo của mùa chưa diễn ra.

## Tệp đầu ra

- `data/models/knn/knn_metrics.json`: metric report KNN.
- `data/models/knn/knn_predictions.csv`: đầu ra KNN nội bộ tương thích với
  luồng Airflow cũ; trang web xếp hạng tương tác không dùng bảng dự đoán tạo sẵn.

Checkpoint được ghi tại:

```text
hdfs://localhost:9000/bigdata/checkpoints/analytics/knn_league_table/latest.json
hdfs://localhost:9000/bigdata/checkpoints/analytics/knn_league_table/history.jsonl
```

Airflow lấy dữ liệu tổng kết đội/mùa từ `fact_team_season` và phong độ trận đấu
từ `fact_team_match`. Nếu dữ liệu trận đang chạy chưa có lịch sử năm trận, KNN
nội bộ dùng PPG làm giá trị thay thế; mẫu lịch sử hoàn tất dùng Recent Form tính
trực tiếp từ năm kết quả cuối mùa.
