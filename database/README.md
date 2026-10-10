# Neon reporting schema

`schemas/analytics.sql` is the idempotent serving schema used by the Airflow
publication and FastAPI. It stores run metadata, complete analytical report
payloads, active report pointers, and pipeline-stage status. Raw and Silver
records remain in HDFS.
