from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


TYPE_MAP = {
    "string": StringType,
    "integer": IntegerType,
    "long": LongType,
    "double": DoubleType,
    "boolean": BooleanType,
    "date": DateType,
}


def repo_root() -> Path:
    configured = os.environ.get("PLCA_REPO_ROOT")
    if configured:
        return Path(configured).resolve()
    return Path(__file__).resolve().parents[4]


class SparkRuntime:
    def __init__(self) -> None:
        self.root = repo_root()
        self.silver_root = Path(
            os.environ.get("PLCA_SILVER_ROOT", str(self.root / "data" / "lake" / "silver"))
        )
        self.raw_root = Path(
            os.environ.get("PLCA_RAW_ROOT", str(self.root / "data" / "raw"))
        )
        self.spark: SparkSession | None = None
        self.manifest: dict[str, Any] = {}

    def start(self) -> SparkSession:
        if self.spark is not None:
            return self.spark

        manifest_path = self.silver_root / "silver_manifest.json"
        if not manifest_path.is_file():
            raise FileNotFoundError(f"Không tìm thấy manifest Silver: {manifest_path}")
        self.manifest = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
        tables = self.manifest.get("tables", [])
        if len(tables) != 21:
            raise RuntimeError(f"Manifest yêu cầu 21 bảng Silver, hiện có {len(tables)}.")

        self.spark = (
            SparkSession.builder.master("local[*]")
            .appName("PremierLeagueAnalyticsDashboard")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.driver.memory", os.environ.get("PLCA_SPARK_DRIVER_MEMORY", "2g"))
            .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel(os.environ.get("PLCA_SPARK_LOG_LEVEL", "WARN"))

        loaded = 0
        for entry in tables:
            relative_csv = entry.get("csv")
            if not relative_csv:
                raise RuntimeError(f"Thiếu đường dẫn CSV trong manifest: {entry.get('table')}")
            csv_path = self.silver_root / relative_csv
            if not csv_path.is_file():
                raise FileNotFoundError(f"Thiếu bảng Silver: {csv_path}")
            schema = self._schema(entry.get("schema", {}).get("fields", []))
            frame = (
                self.spark.read.schema(schema)
                .option("header", "true")
                .option("encoding", "UTF-8")
                .option("nullValue", "\\N")
                .option("emptyValue", "")
                .option("mode", "FAILFAST")
                .csv(str(csv_path))
            )
            view_name = Path(entry["table"]).name
            frame.createOrReplaceTempView(view_name)
            loaded += 1

        games_path = self.raw_root / "transfermarkt" / "source_tables" / "games.csv"
        if not games_path.is_file():
            raise FileNotFoundError(f"Không tìm thấy bảng vòng đấu Transfermarkt: {games_path}")
        games = (
            self.spark.read.option("header", "true")
            .option("encoding", "UTF-8")
            .option("mode", "PERMISSIVE")
            .csv(str(games_path))
            .select("game_id", "competition_id", "season", "round")
            .where("competition_id = 'GB1'")
        )
        games.createOrReplaceTempView("tm_games")
        club_names = self.spark.sql(
            """
            SELECT club_id, first(club_name, true) AS club_name
            FROM dim_club
            GROUP BY club_id
            """
        )
        club_names.createOrReplaceTempView("club_names")
        print(
            f"Spark sẵn sàng: {loaded} bảng Silver, "
            f"{self.manifest.get('total_rows_across_tables', 0):,} dòng."
        )
        return self.spark

    @staticmethod
    def _schema(fields: list[dict[str, Any]]) -> StructType:
        schema_fields = []
        for field in fields:
            field_type = TYPE_MAP.get(str(field.get("type", "string")).lower())
            if field_type is None:
                raise TypeError(f"Kiểu cột Silver chưa hỗ trợ: {field.get('type')}")
            schema_fields.append(
                StructField(
                    field["name"],
                    field_type(),
                    nullable=bool(field.get("nullable", True)),
                )
            )
        return StructType(schema_fields)

    def stop(self) -> None:
        if self.spark is not None:
            self.spark.stop()
            self.spark = None
