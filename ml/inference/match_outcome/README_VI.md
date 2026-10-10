# Ước tính thắng – hòa – thua

Model dùng thống kê tổng kết trận, không phải dự đoán theo phút. Nhãn luôn theo đội chủ nhà: `home_win`, `draw`, `away_win`. Dữ liệu lịch sử có 1.140 trận, 380 trận mỗi mùa 2022–2023, 2023–2024 và 2024–2025.

`dataset.py` ghép chín loại CSV của mỗi CLB theo mùa/ngày/đối thủ, đối chiếu hai phía, giữ nguyên mọi trường nguồn trong JSON. `feature_catalog.json` ghi vai trò của từng cột. `silver_reconciliation.json` xác nhận đặc trưng và nhãn trùng với `fact_match_enriched` đã chuẩn hóa. Kafka lưu một bản ghi mỗi sự kiện trận, khóa xác định từ nội dung nguồn; bản tin gửi lại cùng khóa và nội dung không được ghi trùng.

Đầu vào gồm kiểm soát bóng, sút, sút trúng đích, độ chính xác sút, penalty thực hiện, thẻ, phạm lỗi, việt vị, tạt bóng, đánh chặn, tắc bóng và đội hình của hai phía. Dùng cả thống kê mỗi đội và chênh lệch. Không dùng bàn thắng, tỉ số, kết quả, sạch lưới, hiệu suất ghi bàn hoặc thống kê thủ môn có thể tái dựng tỉ số. Không tạo số lần chuyền bóng khi nguồn chưa có. Hai cột penalty được hưởng/phải chịu hoàn toàn thiếu trong tập huấn luyện nên bị loại.

Huấn luyện trên mùa 2022–2023 và so sánh ba thuật toán trên cùng đặc trưng: Spark MLlib Multinomial Logistic Regression (regularization 0.01, 0.1, 1), KNN classifier (k=5, 15, 31, 61) và Spark Random Forest (64 cây, độ sâu 3/5/7, có hoặc không cân bằng lớp). Nửa đầu 2023–2024 chọn thuật toán/tham số theo log loss; nửa sau chọn temperature. Mùa 2024–2025 chỉ kiểm thử sau khi chọn xong. Median, trung bình, độ lệch chuẩn và danh mục đội hình đều lấy từ tập huấn luyện. Cột hằng số sau điền thiếu bị loại và có lý do trong `removed_columns`.

Model đang được chọn là Random Forest 64 cây, độ sâu 7; validation log loss 0,867. Kiểm thử: accuracy 57,4%, macro-F1 0,430, log loss 0,912, Brier 0,537; F1 lớp hòa bằng 0. KNN tốt nhất có validation log loss 0,932; Logistic Regression 0,908. Không điều chỉnh theo kết quả kiểm thử.

`data/models/match_outcome/spark_model` là model Spark đã chọn; `logistic_baseline` giữ bản Logistic Regression. `model.json` chứa thứ tự đặc trưng, cấu trúc cây (hoặc hệ số/tham chiếu KNN), median, chuẩn hóa và temperature cho API NumPy. Huấn luyện kiểm tra xác suất Spark và NumPy trên toàn bộ 1.140 trận với sai số tối đa 1e-8. Báo cáo gồm accuracy, macro-F1, log loss, Brier score, confusion matrix và baseline theo tần suất nhãn của tập huấn luyện. `algorithm_comparison.json` ghi đủ 13 cấu hình đã thử. Khả năng phân biệt hòa còn yếu; xem F1 lớp hòa trước khi diễn giải kết quả.

API công bố model qua Neon sau khi lưu đủ báo cáo. Bản tin dự đoán giữ phiên bản model đã dùng; Neon phân biệt kết quả bằng khóa sự kiện cộng phiên bản, lưu được lịch sử nhiều model. Spark ghi HDFS trước, ghi Neon sau, rồi mới hoàn tất checkpoint. Khi một dịch vụ lỗi, job thất bại và Airflow thử lại.

Chạy toàn hệ thống theo `infrastructure/docker/README_VI.md`. Chạy lại bằng DAG `football_pipeline`; DAG `football_kafka_analytics` tiêu thụ Kafka bằng `availableNow` mỗi phút, chung pool một slot Spark.
