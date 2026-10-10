# Công cụ dữ liệu và đóng gói

Kiến trúc và cách chạy nằm ở [README gốc](../README.md). Hạ tầng Kafka, Airflow, Neo4j và Hadoop được tái sử dụng; Compose production chỉ chứa ứng dụng. Script mã nguồn ở `scripts/` chỉ khởi động container hạ tầng đã có.

- `package_portable.py`: tạo `artifacts/PremierLeague_Docker.zip`, manifest và SHA256; gồm script chạy, seed/model và mã nguồn, loại secret/cache/image tar/archive Hadoop.
- `export_portable_seed.py`: xuất Silver, model và KNN làm seed bootstrap.
- `expand_raw.py`: giải nén nguồn lớn từ gzip và đối chiếu checksum trước import/build.
- `integrate_sources.py`: đối chiếu schema/checksum và tích hợp dữ liệu từ các thư mục nguồn.

```bash
python infra/package_portable.py
python portable/verify_bundle.py --archive artifacts/PremierLeague_Docker.zip
```

Đóng gói tìm root theo vị trí script, chạy được từ thư mục làm việc khác và không sinh `delivery/delivery`. Xem [hướng dẫn triển khai](../portable/README_VI.md) và [giới hạn nghiệm thu](../validation/README.md).
