# Data layout

- `raw/`: FBref, Transfermarkt and DataMB source snapshots used by ingestion. The large appearances file is stored compressed and expanded with checksum verification by `infrastructure/scripts/build/expand_raw.py`.
- `lake/silver/`: a small, validated CSV snapshot used for local analysis and tests. Production Silver and Gold tables live in HDFS, not in this directory or Neon.
- `samples/pipeline-seed/`: packaged CI seed for HDFS and model checks.
- `models/`: versioned match-model artifacts and KNN output used by tests and the packaged pipeline.

The path, schema, source and checksum manifests live beside this data. Do not place secrets, Hadoop volumes, or local database volumes in this tracked directory.
