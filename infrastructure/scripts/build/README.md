# Công cụ dữ liệu và đóng gói

Kiến trúc và cách chạy nằm ở [README gốc](../../../README.md). Hạ tầng Kafka, Airflow, HDFS và Neon được cấu hình như dịch vụ ngoài. Script mã nguồn ở `../launchers/` chỉ khởi động container hạ tầng đã có.

- `package_portable.py`: tạo `artifacts/PremierLeague_Docker.zip`, manifest và SHA256; gồm script chạy, seed/model và mã nguồn, loại secret/cache/image tar/archive Hadoop.
- `export_portable_seed.py`: xuất Silver, model và KNN làm seed bootstrap.
- `expand_raw.py`: giải nén nguồn lớn từ gzip và đối chiếu checksum trước import/build.
- `integrate_sources.py`: đối chiếu schema/checksum và tích hợp dữ liệu từ các thư mục nguồn.

```bash
python infrastructure/scripts/build/package_portable.py
python infrastructure/docker/verify_bundle.py --archive artifacts/PremierLeague_Docker.zip
```

Đóng gói tìm root theo vị trí script, chạy được từ thư mục làm việc khác và không sinh `delivery/delivery`. Xem [hướng dẫn triển khai](../../docker/README_VI.md) và [giới hạn nghiệm thu](../../../docs/validation/README.md).
