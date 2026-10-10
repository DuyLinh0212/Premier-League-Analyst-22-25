# Football Analytics

Next.js hiển thị hai màn hình: KNN đội bóng và ước tính thắng–hòa–thua từ thống kê tổng kết trận. Proxy `/api/football/*` gọi API Python; mật khẩu Neo4j không đi vào trình duyệt.

Khởi động đầy đủ bằng `portable/start.ps1` hoặc `portable/start.sh` ở gốc gói. Xem `portable/README_VI.md` để cấu hình Docker và chạy trên máy khác.

Phát triển web riêng: `npm ci`, đặt `FOOTBALL_API_URL=http://localhost:8000`, rồi `npm run dev`. API và Kafka cần đang chạy để gửi dự đoán. `npm run build` và `npm start` dùng cho bản production.
