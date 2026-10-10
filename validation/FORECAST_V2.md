# Dự đoán trước trận và trong trận

Hai API độc lập: `/predict` tiếp tục chạy model thống kê tổng kết; `/predict/v2` dùng lịch sử trước ngày dự đoán và trạng thái trận. Không so sánh accuracy của hai model như cùng một bài toán.

## Đầu vào và dữ liệu

`POST /predict/v2`: `home_team`, `away_team`, `prediction_date` (YYYY-MM-DD), `mode` (`pregame` hoặc `live`). Chế độ live thêm `home_goals`, `away_goals`, `minute` (0–90), `stoppage_minutes` (chỉ tại 45/90), `finished`, `home_stats`, `away_stats`. Mặc định 0–0, phút 0, chưa kết thúc. Các thống kê là tùy chọn; null/không gửi khác số 0. Hai giá trị possession nếu đều có phải cộng thành 100%.

Ngày mặc định là ngày sau trận cuối dataset. Model đã chọn và hiệu chỉnh bằng mùa 2023/24 nên API từ chối ngày không muộn hơn `trained_through`; không dùng model biết tương lai để dự đoán một trận cũ. Lịch sử, đối đầu và láng giềng luôn có ngày nhỏ hơn ngày yêu cầu, tối đa ba mùa. Loại cả ngày mục tiêu vì dataset không có giờ bắt đầu đầy đủ.

`GET /prediction/config` trả mốc dữ liệu, ngày tối thiểu/mặc định, phiên bản và thống kê hỗ trợ. `GET /model/metrics/v2` trả các bộ đánh giá riêng. `GET /reports/{run_id}/match_prediction_v2` mở lại kết quả đã lưu; model lịch sử được đọc bằng phiên bản đã công bố.

## Model và độ tương đồng

Poisson có regularization, link log; các đóng góp `exp(coef × feature_chuẩn_hóa)` là hệ số cho số bàn kỳ vọng, không cộng trực tiếp phần trăm thắng. Đặc trưng gồm sức tấn công/phòng ngự, điểm phong độ 5 trận, hiệu số 10 trận, thành tích theo sân, nghỉ giữa trận, lối chơi và đối đầu được làm trơn với 6 trận trung tính. CLB ít dữ liệu dùng mức trung bình có làm trơn.

Cosine dùng vector chuẩn hóa Min–Max từ tập train, kết hợp cả hai đội và làm trơn phân bố W/D/L. K, thời gian bán rã và trọng số được chọn trên 190 trận đầu mùa 2023/24; temperature dùng 190 trận sau. Không có bonus cố định cho bất kỳ CLB nào. Cosine là độ giống vector, không phải xác suất thắng; zero vector không được gán 100% tương đồng.

Model logistic đa lớp trạng thái trận học từ tỉ số, thời gian, xác suất trước trận và tương tác tỉ số–phút. 3.445 goal events đối chiếu khớp 1.140 tỉ số; club_id của goal event là đội được cộng bàn kể cả phản lưới, không đảo lại lần nữa. Bù giờ hiệp một đứng trước các sự kiện hiệp hai.

Thống kê live nhập được so với vector thống kê cuối trận lịch sử; số đếm quy đổi /90, phần trăm giữ nguyên, chỉ dùng trường có giá trị. Trọng số giảm đầu trận và cuối trận để tránh át tỉ số. Trọng số thống kê được chọn bằng **association của thống kê tổng kết**, không có chứng cứ calibration đối với possession/shots theo phút. Response và Web luôn phân biệt điều này. Các chỉnh sửa không được nhập vào tập train lịch sử. Phút 90 chưa tự động kết thúc; `finished=true` trả kết quả 100% theo tỉ số.

## Đánh giá và triển khai

2022/23: train 380 trận; 2023/24: tune 190 + calibration 190; 2024/25: test 380 trận. Live có 9 trạng thái/trận, cùng split với trận, không phải 3.420 trận độc lập. Báo cáo có accuracy, macro-F1, log loss, Brier, confusion matrix, reliability bins, kết quả theo phút và đội đang dẫn/hòa. Thống kê tổng kết được đánh giá riêng dưới tên `statistics_association`.

Airflow thêm `match_forecast_v2` giữa `match_outcome` và `publish`; đọc goal events Silver thật, lưu artifact/metrics vào HDFS và đọc lại trước khi hoàn tất. Neo4j chỉ công bố khi toàn bộ bundle hoàn chỉnh. Kafka schema 2 đóng băng phiên bản model, dataset fingerprint và ngày dự đoán; replay đọc đúng bản đã công bố, schema 1 vẫn được hỗ trợ.

Huấn luyện fixture: `PYTHONPATH=Generate_LeagueTable_Api/analystic python -m match_outcome.train_v2`. Dependencies không bổ sung; NumPy được khóa trong runtime. CI huấn luyện lại, build ứng dụng, import DAG, kiểm thử Neo4j/Kafka/replay/recovery và Web sau khi bỏ báo cáo cục bộ; xuất ZIP và screenshot. Máy local chỉ dùng container Airflow/Spark/Kafka hiện có. Không build/pull image, tạo container hay thêm Hadoop.

Neo4j Desktop local đang cần `NEO4J_PASSWORD` trong `portable/.env` để công bố dữ liệu trên máy này. Không đưa mật khẩu vào Git hoặc ZIP. Kiểm thử Database thực trên CI không thay thế nghiệm thu Neo4j Desktop khi chưa có cấu hình này; chuyển máy thực tế vẫn cần máy nhận.
