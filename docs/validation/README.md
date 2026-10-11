# Kiểm thử và ranh giới dữ liệu

GitHub Actions dùng PostgreSQL tạm tương thích Neon để kiểm tra schema, xuất bản report, API, lịch sử và xử lý lỗi kết nối. Khi triển khai thật, ứng dụng chỉ dùng `NEON_DATABASE_URL` và PostgreSQL do Neon cấp.

Các bảng Bronze/Silver và dữ liệu đầu vào tiếp tục nằm trong HDFS. Neon chỉ giữ lần chạy, trạng thái pipeline, report phân tích, phiên bản model và provenance. CI kiểm tra không có report dạng `raw/*` hoặc `facts/*` bị đưa vào Neon.

CI chạy fixture HDFS/Kafka cùng với kiểm thử API/Web, replay và restart. Việc này không thay thế kiểm chứng kết nối tới HDFS thật của máy cá nhân. Gói portable dùng cấu hình Neon từ `.env`; không chứa secret.
