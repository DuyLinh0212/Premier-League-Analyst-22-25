# Dữ liệu Silver Premier League đã chuẩn hóa

Bản xuất ngày 2026-10-08T02:05:26+00:00, từ snapshot đã nghiệm thu `plca_954634acee6255faafab8270`.
Bao gồm toàn bộ 21 bảng Silver của 3 mùa 2022-2023, 2023-2024, 2024-2025.
Đây là dữ liệu sau chuẩn hóa, trước các phân tích Gold, không phải dữ liệu Web.

- `csv/`: mỗi bảng là một CSV đầy đủ, không lấy mẫu, UTF-8 có BOM.
- `parquet/`: bản sao nguyên cây Parquet Snappy từ HDFS, giữ kiểu dữ liệu gốc.
- `silver_manifest.json`: nguồn HDFS, khóa, schema từng cột, số dòng theo mùa và số NULL.
- `TABLES.csv`: danh mục bảng, số dòng, cột và đường dẫn.
- `metadata/`: kết quả kiểm tra chất lượng và kiểm tra độc lập đã nghiệm thu.
- `SHA256SUMS.txt`: mã kiểm tra toàn bộ file trong gói.

CSV dùng chuỗi `\N` để biểu diễn NULL. Chuỗi rỗng là chuỗi rỗng, số 0 vẫn là 0.
Không tự động thay giá trị thiếu bằng 0. Decimal không bị chuyển sang float.
Giữ ID ở dạng văn bản khi nhập Excel. Ưu tiên Parquet nếu cần kiểu dữ liệu chính xác.
CSV là dữ liệu thô để trao đổi, không phải workbook có công thức.

Quan hệ cầu thủ–CLB là theo `player_id + club_id + season` từ nguồn FBref;
không gán CLB lịch sử từ `current_club` của Transfermarkt. Một cầu thủ có thể có
nhiều CLB trong một mùa. `fact_player_playing_time` và bridge bao gồm danh sách
đăng ký; `fact_player_season` là bộ thống kê chuẩn hóa tương ứng nguồn chuẩn.
ID `fbref:<hash>` là khóa nội bộ tên và năm sinh vì CSV không có ID FBref gốc.
Các trường hợp chưa ghép được Transfermarkt được giữ trong `audit/identity_review`.
`dim_club` còn có CLB ngoài Premier League để nối chuyển nhượng; không chỉ có 20 CLB.
Các bảng sự kiện và dimension có grain khác nhau; không cộng tổng dòng thành số người.

Để gửi lại: đính kèm nguyên file ZIP. Nếu nơi gửi không hỗ trợ ZIP,
giải nén và gửi thư mục `csv/` cùng `silver_manifest.json` và `README_VI.md`.
Việc xuất chỉ đọc HDFS; không chạy lại pipeline và không thay đổi ID hoặc dữ liệu.
