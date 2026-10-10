# Premier League: KNN và ước tính kết quả trận

Hệ thống hiện tại dùng Docker Compose tại `portable/compose.yaml`. Chạy `portable/start.ps1` trên Windows hoặc `bash portable/start.sh` trên Linux. Hướng dẫn đầy đủ: `portable/README_VI.md` ở gốc workspace/gói bàn giao.

Giữ dữ liệu gốc FBref, Transfermarkt, chuẩn hóa Silver và Kafka. Hai phân tích hiện hành là KNN tại `Generate_LeagueTable_Api/analystic/knn` và `match_outcome` cùng thư mục. Match outcome so sánh Logistic Regression, KNN classifier và Random Forest; model được chọn bằng log loss validation trước khi kiểm thử. Web Next.js nằm tại `Premier Leauge Club Analyst/web-next`.

HDFS dùng block size 67.108.864 byte, replication 2, một NameNode và ba DataNode. Các job Spark dùng chung một slot Airflow để phù hợp giới hạn Docker 6 GiB RAM.
