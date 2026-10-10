"""Fetch exact Kafka connector artifacts at image build, never at task startup."""
import hashlib
import json
import os
import urllib.request
from pathlib import Path
ARTIFACTS = [
    ("org.apache.spark", "spark-sql-kafka-0-10_2.12", "3.5.1"),
    ("org.apache.spark", "spark-token-provider-kafka-0-10_2.12", "3.5.1"),
    ("org.apache.kafka", "kafka-clients", "3.4.1"),
    ("org.apache.commons", "commons-pool2", "2.11.1"),
    ("com.github.luben", "zstd-jni", "1.5.2-1"),
    ("org.lz4", "lz4-java", "1.8.0"),
    ("org.xerial.snappy", "snappy-java", "1.1.10.3"),
]
directory = Path(os.getenv('KAFKA_JARS_DIR', '/opt/kafka-jars'))
directory.mkdir(parents=True, exist_ok=True)
manifest = []
for group, artifact, version in ARTIFACTS:
    name = f"{artifact}-{version}.jar"
    url = f"https://repo.maven.apache.org/maven2/{group.replace('.', '/')}/{artifact}/{version}/{name}"
    data = urllib.request.urlopen(url, timeout=120).read()
    expected = urllib.request.urlopen(url + ".sha1", timeout=60).read().decode().split()[0]
    if hashlib.sha1(data).hexdigest() != expected:
        raise RuntimeError("Maven checksum mismatch: " + name)
    (directory / name).write_bytes(data)
    manifest.append({"file": name, "sha256": hashlib.sha256(data).hexdigest(), "url": url})
(directory / "manifest.json").write_text(json.dumps(manifest, indent=2))
