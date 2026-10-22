#!/usr/bin/env python3
"""Normalize HDFS Bronze CSV/JSON sources to one-to-one Silver Parquet datasets.

The transforms are generated as Spark SQL expressions so that the normalization
rules run in the Spark engine. Source columns and files are never modified.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import logging
from pathlib import Path, PurePosixPath
import re
import sys
import unicodedata
import uuid
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import ArrayType, MapType, StructType

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.hdfs_common import hdfs_root  # noqa: E402

LOGGER = logging.getLogger("silver_normalize")
SUPPORTED_EXTENSIONS = (".csv.gz", ".csv", ".json")
GZIP_MAGIC = (0x1F, 0x8B)

NULL_TOKENS = ("", "-", "—", "–", "na", "n/a", "null", "none", "nan")
INTEGER_FIELDS = {
    "rk", "number_pl", "mp", "w", "d", "l", "gf", "ga", "gd", "pts",
    "born", "year", "last_season", "min", "mins", "minutes",
    "starts", "compl", "subs", "unsub", "ong", "onga", "gls", "ast",
    "pk", "pkatt", "pka", "pksv", "pkm", "crdy", "crdr", "2crdy",
    "fls", "fld", "off", "crs", "int", "tklw", "pkwon", "pkcon", "og",
    "sota", "saves", "cs", "attendance", "stadium_seats", "squad_size",
    "foreigners_number", "national_team_players", "international_caps",
    "international_goals", "height_in_cm",
    "ga_gk", "ga_match", "gf_match",
}
INTEGER_SUFFIXES = (
    "_count", "_number", "_size", "_minutes", "_goals", "_assists",
    "_starts", "_matches_played", "_games_played", "_shots",
    "_shots_on_target", "_saves", "_clean_sheets", "_cards",
    "_yellow_cards", "_red_cards", "_second_yellow_cards",
    "_fouls_committed", "_fouls_drawn", "_offsides", "_crosses",
    "_interceptions", "_tackles_won", "_penalties_won",
    "_penalties_conceded", "_own_goals", "_penalties_attempted",
    "_penalties_saved", "_penalties_missed", "_goals_for",
    "_goals_against", "_wins", "_draws", "_losses", "_points",
    "_attendance", "_seats", "_caps",
)
DECIMAL_FIELDS = {
    "age", "90s", "field_90s", "ga90", "ppm", "val", "poss",
    "pct_estimated", "wage_estimated_pct",
    "pts_per_mp", "min_pct", "mn_mp", "mn_start", "mn_sub",
    "on_off", "plus_minus", "plus_minus_90", "g_a", "g_pk",
    "g_a_pk", "gls_per90", "ast_per90", "g_a_per90", "g_pk_per90",
    "g_a_pk_per90", "sh_per90", "sot_per90", "g_per_sh",
    "g_per_sot", "pk_save_pct",
}
DECIMAL_MARKERS = (
    "_pct", "_percent", "_per90", "_per_90", "_per_mp", "_per_match",
    "_per_start", "_per_sub", "possession", "squad_age", "average_age",
    "market_value", "_eur", "_gbp", "_usd", "wage", "fee", "transfer_record",
    "points_per", "goal_diff", "goal_difference", "rate", "ratio",
)
STRING_FIELDS = {
    "id", "season", "transfer_season", "result", "score", "time", "day",
    "round", "venue", "position", "pos", "nation", "foot", "url", "notes",
    "formation", "op_formation", "opponent_formation", "competition_type",
    "competition_id", "domestic_competition_id", "filename", "club_code",
    "player_code", "date_of_birth", "contract_expiration_date",
}
NAME_FIELDS = {
    "name", "player", "player_name", "first_name", "last_name", "squad",
    "club", "club_name", "team", "team_name", "home_team", "away_team",
    "opponent", "scorer", "assist", "goalkeeper",
    "captain", "referee", "coach_name", "home_club_manager_name",
    "away_club_manager_name", "home_club_name", "away_club_name",
    "current_club_name", "from_club_name", "to_club_name", "stadium",
    "stadium_name", "agent_name", "city_of_birth", "country_of_birth",
    "country_of_citizenship",
}
WAGE_FIELDS = {"weekly_wages", "annual_wages"}
EURO_AMOUNT_FIELDS = {"transfer_fee", "net_transfer_record", "total_market_value"}

_TRANSLATION = []
for _codepoint in range(192, 592):
    _character = chr(_codepoint)
    _ascii = "".join(
        char
        for char in unicodedata.normalize("NFKD", _character)
        if not unicodedata.combining(char)
    )
    if len(_ascii) == 1 and _ascii.isascii() and _ascii.isalpha():
        _TRANSLATION.append((_character, _ascii))
TRANSLATE_FROM = "".join(source for source, _ in _TRANSLATION)
TRANSLATE_TO = "".join(target for _, target in _TRANSLATION)


def sql_literal(value: str) -> str:
    """Quote a SQL string literal."""
    escaped = value.replace("\\", "\\\\").replace("'", "''")
    return "'" + escaped + "'"


def sql_identifier(value: str) -> str:
    quote = chr(96)
    return quote + value.replace(quote, quote + quote) + quote


def normalize_column_name(value: str) -> str:
    """Convert source headers to stable lowercase snake_case names."""
    value = unicodedata.normalize("NFKD", value.lstrip("\ufeff")).strip()
    value = "".join(char for char in value if not unicodedata.combining(char))
    value = value.replace("+/-", " plus minus ")
    value = value.replace("#", " number ")
    value = value.replace("%", " pct ")
    value = value.replace("/", " per ")
    value = value.replace("+", " ")
    value = re.sub(r"[^A-Za-z0-9]+", "_", value).strip("_").lower()
    value = re.sub(r"_+", "_", value)
    if not value:
        value = "column"
    if value[0].isdigit():
        value = "field_" + value
    return value


def unique_column_names(names: list[str]) -> list[str]:
    """Keep normalized names unique even if a source has duplicate headings."""
    used: set[str] = set()
    result: list[str] = []
    for name in names:
        candidate = name
        suffix = 2
        while candidate in used:
            candidate = f"{name}_{suffix}"
            suffix += 1
        result.append(candidate)
        used.add(candidate)
    return result


def flatten_source(frame: DataFrame) -> DataFrame:
    """Flatten JSON structs and make every source field name SQL-safe."""
    columns = []

    def visit(field: Any, parts: tuple[str, ...]) -> None:
        field_type = field.dataType
        path = parts + (field.name,)
        if isinstance(field_type, StructType):
            for child in field_type.fields:
                visit(child, path)
            return

        output_name = normalize_column_name("_".join(path))
        reference = ".".join(sql_identifier(part) for part in path)
        if isinstance(field_type, (ArrayType, MapType)):
            expression = f"to_json({reference})"
        else:
            expression = f"CAST({reference} AS STRING)"
        columns.append((output_name, expression))

    for source_field in frame.schema.fields:
        visit(source_field, ())

    if not columns:
        raise ValueError("Nguồn không có cột dữ liệu.")

    names = unique_column_names([name for name, _ in columns])
    select_sql = ",\n".join(
        f"{expression} AS {sql_identifier(name)}"
        for (_, expression), name in zip(columns, names, strict=True)
    )
    frame.createOrReplaceTempView("bronze_normalize_source")
    return frame.sparkSession.sql(f"SELECT {select_sql} FROM bronze_normalize_source")


def clean_expression(column_name: str) -> str:
    """Trim whitespace and convert known missing-value markers to SQL NULL."""
    raw = f"CAST({sql_identifier(column_name)} AS STRING)"
    clean = (
        f"trim(regexp_replace("
        f"replace(replace(replace({raw}, ' ', ' '), '　', ' '), '​', ''), "
        f"'\\\\s+', ' '))"
    )
    tokens = ", ".join(sql_literal(token) for token in NULL_TOKENS)
    return f"CASE WHEN {raw} IS NULL OR lower({clean}) IN ({tokens}) " \
           f"THEN CAST(NULL AS STRING) ELSE {clean} END"


def is_identifier_or_label(name: str) -> bool:
    return (
        name in STRING_FIELDS
        or name == "name_key"
        or name.endswith("_id")
        or name.endswith("_code")
        or name.endswith("_key")
        or name.startswith("url")
        or name.startswith("notes")
        or name.endswith("_url")
        or name.endswith("_name")
        or name.endswith("_position")
    )


def numeric_type(name: str) -> str | None:
    """Return an explicit Silver numeric type for known metric/count columns."""
    if is_identifier_or_label(name) or name in {"start", "is_own_goal"}:
        return None
    if name in DECIMAL_FIELDS or any(marker in name for marker in DECIMAL_MARKERS):
        return "DOUBLE"
    if name in INTEGER_FIELDS:
        return "BIGINT"
    if name.endswith(INTEGER_SUFFIXES):
        return "BIGINT"

    # Common FBref abbreviations become numeric after header normalization.
    if re.fullmatch(
        r"(?:field_)?(?:2crdy|pkatt|pka|pksv|pkm|crdy|crdr|fls|fld|off|crs|int|tklw|og|sota|saves|cs)",
        name,
    ):
        return "BIGINT"
    if re.fullmatch(r"(?:plus_minus|g_a|g_pk|g_a_pk)(?:_per_?90)?", name):
        return "DOUBLE"
    if re.fullmatch(r"(?:raw_|pct_)?[a-z0-9_]+_p90", name):
        return "DOUBLE"
    if name in {"age", "average_age", "squad_age"}:
        return "DOUBLE"
    if name in {"last_season", "birth_year"}:
        return "BIGINT"
    return None


def numeric_expression(clean: str, kind: str) -> str:
    value = f"regexp_replace({clean}, ',', '')"
    value = f"regexp_replace({value}, '%$', '')"
    return f"try_cast({value} AS {kind})"


def date_expression(clean: str) -> str:
    iso_date = (
        f"CASE WHEN {clean} RLIKE '^\\\\d{{4}}-\\\\d{{2}}-\\\\d{{2}}' "
        f"THEN substring({clean}, 1, 10) ELSE {clean} END"
    )
    return f"try_cast({iso_date} AS DATE)"


def boolean_expression(clean: str) -> str:
    lowered = f"lower({clean})"
    return (
        f"CASE WHEN {lowered} IN ('true', '1', 'yes', 'y') THEN TRUE "
        f"WHEN {lowered} IN ('false', '0', 'no', 'n') THEN FALSE "
        "ELSE CAST(NULL AS BOOLEAN) END"
    )


def name_key_expression(clean: str) -> str:
    translated = (
        f"translate(regexp_replace({clean}, '&', ' and '), "
        f"{sql_literal(TRANSLATE_FROM)}, {sql_literal(TRANSLATE_TO)})"
    )
    key = f"trim(regexp_replace(lower({translated}), '[^a-z0-9]+', ' '))"
    return f"CASE WHEN length({key}) = 0 THEN CAST(NULL AS STRING) ELSE {key} END"


def euro_amount_expression(clean: str) -> str:
    pattern = r"([+-]?)\s*(?:€|EUR|£|GBP|\$|USD)?\s*([0-9][0-9,]*(?:\.[0-9]+)?)\s*(bn|BN|[kKmMbB])?"
    sign = f"regexp_extract({clean}, {sql_literal(pattern)}, 1)"
    amount = f"regexp_extract({clean}, {sql_literal(pattern)}, 2)"
    scale = f"upper(regexp_extract({clean}, {sql_literal(pattern)}, 3))"
    number = f"try_cast(regexp_replace({amount}, ',', '') AS DOUBLE)"
    multiplier = (
        f"CASE {scale} WHEN 'K' THEN 1000.0 WHEN 'M' THEN 1000000.0 "
        "WHEN 'B' THEN 1000000000.0 WHEN 'BN' THEN 1000000000.0 ELSE 1.0 END"
    )
    sign_multiplier = f"CASE {sign} WHEN '-' THEN -1.0 ELSE 1.0 END"
    return (
        f"CASE WHEN {amount} = '' THEN CAST(NULL AS DOUBLE) "
        f"ELSE {number} * {multiplier} * {sign_multiplier} END"
    )


def currency_expression(clean: str, currency: str) -> str:
    symbol = {"gbp": "£", "eur": "€", "usd": r"\$"}[currency]
    pattern = rf"(?:{symbol}|{currency.upper()})\s*([0-9][0-9,]*(?:\.[0-9]+)?)"
    amount = f"regexp_extract({clean}, {sql_literal(pattern)}, 1)"
    return (
        f"CASE WHEN {amount} = '' THEN CAST(NULL AS DOUBLE) "
        f"ELSE try_cast(regexp_replace({amount}, ',', '') AS DOUBLE) END"
    )


def wants_name_key(name: str) -> bool:
    return name in NAME_FIELDS or name.endswith("_name")


def normalized_sql(frame: DataFrame, metadata: dict[str, str | None]) -> DataFrame:
    """Clean and type source rows using SQL, with conservative field rules."""
    names = list(frame.columns)
    selections: list[str] = []
    output_names: set[str] = set()

    def append(expression: str, name: str) -> None:
        unique = name
        suffix = 2
        while unique in output_names:
            unique = f"{name}_{suffix}"
            suffix += 1
        selections.append(f"{expression} AS {sql_identifier(unique)}")
        output_names.add(unique)

    frame.createOrReplaceTempView("bronze_normalize_source")
    for name in names:
        clean = clean_expression(name)

        if name in WAGE_FIELDS:
            append(clean, name + "_raw")
            base = name.removesuffix("s")
            for currency in ("gbp", "eur", "usd"):
                append(currency_expression(clean, currency), base + "_" + currency)
            continue

        if name in EURO_AMOUNT_FIELDS:
            append(clean, name + "_raw")
            amount_name = {
                "transfer_fee": "transfer_fee_eur",
                "net_transfer_record": "net_transfer_record_eur",
                "total_market_value": "total_market_value_eur",
            }[name]
            amount = euro_amount_expression(clean)
            append(amount, amount_name)
            if name == "transfer_fee":
                status = (
                    f"CASE WHEN {clean} IS NULL THEN CAST(NULL AS STRING) "
                    f"WHEN lower({clean}) RLIKE 'free|without fee' THEN 'free' "
                    f"WHEN lower({clean}) RLIKE 'loan' THEN 'loan' "
                    f"WHEN {amount} IS NULL THEN 'undisclosed_or_non_numeric' "
                    f"WHEN {amount} = 0 THEN 'zero_fee' ELSE 'reported_amount' END"
                )
                append(status, "transfer_fee_status")
            continue

        if name == "minute":
            append(clean, name)
            base_minute = f"regexp_extract({clean}, '^[0-9]+', 0)"
            added_minute = f"regexp_extract({clean}, '^[0-9]+.*[+]([0-9]+)$', 1)"
            append(f"try_cast({base_minute} AS BIGINT)", "minute_base")
            append(f"try_cast({added_minute} AS BIGINT)", "minute_added")
            continue

        if name in {"date", "match_date", "birth_date", "date_of_birth",
                    "transfer_date", "valuation_date", "appearance_date",
                    "contract_expiration_date"}:
            value = date_expression(clean)
        elif name == "start" or name == "is_own_goal" or name.startswith("is_") or name.endswith("_active"):
            value = boolean_expression(clean)
        else:
            kind = numeric_type(name)
            value = numeric_expression(clean, kind) if kind else clean
        append(value, name)

        if wants_name_key(name) and name + "_key" not in names:
            append(name_key_expression(clean), name + "_key")

    append(sql_literal(metadata["source_system"] or "unknown"), "_source_system")
    append(sql_literal(metadata["source_file"] or ""), "_source_file")
    append(sql_literal(metadata["source_table"] or ""), "_source_table")
    season = metadata.get("source_season")
    append(
        f"CAST({sql_literal(season)} AS STRING)" if season else "CAST(NULL AS STRING)",
        "_source_season",
    )
    team = metadata.get("source_team")
    append(
        f"CAST({sql_literal(team)} AS STRING)" if team else "CAST(NULL AS STRING)",
        "_source_team",
    )
    append("current_timestamp()", "_ingested_at_utc")

    return frame.sparkSession.sql(
        "SELECT\n  " + ",\n  ".join(selections) + "\nFROM bronze_normalize_source"
    )


def path_extension(path: str) -> str | None:
    lowered = path.lower()
    return next((suffix for suffix in SUPPORTED_EXTENSIONS if lowered.endswith(suffix)), None)


def relative_path(root: str, child: str) -> str:
    prefix = root.rstrip("/") + "/"
    if not child.startswith(prefix):
        raise ValueError(f"Tệp nằm ngoài Bronze root: {child}")
    return child[len(prefix):]


def output_relative_path(source_relative: str) -> str:
    extension = path_extension(source_relative)
    if extension is None:
        raise ValueError(f"Không hỗ trợ định dạng nguồn: {source_relative}")
    return source_relative[:-len(extension)] + ".parquet"


def metadata_for(source_relative: str) -> dict[str, str | None]:
    relative = PurePosixPath(source_relative)
    parts = relative.parts
    extension = path_extension(source_relative)
    filename = relative.name[:-len(extension)] if extension else relative.stem
    source_season = next((part for part in parts if re.fullmatch(r"20\d{2}-20\d{2}", part)), None)
    source_team = None
    if "04_teams" in parts:
        section_index = parts.index("04_teams")
        if section_index + 1 < len(parts):
            source_team = parts[section_index + 1]
    return {
        "source_system": parts[0] if parts else "unknown",
        "source_file": source_relative,
        "source_table": filename,
        "source_season": source_season,
        "source_team": source_team,
    }


def list_source_files(spark: SparkSession, bronze_root: str) -> tuple[list[tuple[str, str]], list[str]]:
    jvm = spark._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    root_path = jvm.org.apache.hadoop.fs.Path(bronze_root)
    root_fs = root_path.getFileSystem(conf)
    if not root_fs.exists(root_path):
        raise FileNotFoundError(f"Không tìm thấy Bronze: {bronze_root}")
    qualified_root = root_path.makeQualified(root_fs.getUri(), root_fs.getWorkingDirectory()).toString().rstrip("/")

    sources: list[tuple[str, str]] = []
    unsupported: list[str] = []

    def walk(path: Any) -> None:
        for status in root_fs.listStatus(path):
            item_path = status.getPath()
            name = item_path.getName()
            if name.startswith((".", "_")):
                continue
            if status.isDirectory():
                walk(item_path)
                continue
            if not status.isFile():
                continue
            qualified = item_path.makeQualified(root_fs.getUri(), root_fs.getWorkingDirectory()).toString()
            relative = relative_path(qualified_root, qualified)
            if path_extension(relative):
                sources.append((qualified, relative))
            else:
                unsupported.append(relative)

    walk(root_path)
    sources.sort(key=lambda item: item[1].lower())
    unsupported.sort()
    return sources, unsupported


def verify_gzip_headers(spark: SparkSession, sources: list[tuple[str, str]]) -> None:
    """Fail before writing if a .gz file is not actually gzip-compressed."""
    jvm = spark._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    for source_path, relative in sources:
        if not relative.lower().endswith(".gz"):
            continue
        path = jvm.org.apache.hadoop.fs.Path(source_path)
        fs = path.getFileSystem(conf)
        stream = fs.open(path)
        try:
            first = int(stream.read())
            second = int(stream.read())
        finally:
            stream.close()
        if (first, second) != GZIP_MAGIC:
            rendered = " ".join(f"{byte & 0xff:02x}" for byte in (first, second))
            raise ValueError(
                f"{relative} có đuôi .gz nhưng header là [{rendered}], "
                "không phải gzip 1f 8b. Hãy khôi phục/tải lại file Bronze nguồn."
            )


def ensure_distinct_roots(bronze: str, silver: str) -> None:
    source = bronze.rstrip("/")
    target = silver.rstrip("/")
    if source == target or target.startswith(source + "/"):
        raise ValueError("Silver không được nằm trong Bronze; hãy chọn hai thư mục riêng.")


def write_manifest(spark: SparkSession, manifest_path: str, payload: dict[str, Any]) -> None:
    text = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    spark.createDataFrame([(text,)], ["value"]).coalesce(1).write.text(manifest_path)


def read_json_source(spark: SparkSession, source_path: str) -> DataFrame:
    """Read JSON Lines, one JSON object, or a top-level JSON array."""
    whole_file = spark.read.option("wholetext", "true").text(source_path)
    first_row = whole_file.first()
    if first_row is None or first_row["value"] is None:
        raise ValueError(f"Tệp JSON rỗng: {source_path}")

    try:
        root = json.loads(first_row["value"])
    except json.JSONDecodeError:
        # JSON Lines: Spark reads and infers all records without collecting them.
        return spark.read.option("mode", "FAILFAST").json(source_path)

    if isinstance(root, dict):
        # A single pretty-printed JSON object.
        return spark.read.option("multiLine", "true").option("mode", "FAILFAST").json(source_path)
    if not isinstance(root, list):
        raise ValueError(f"JSON phải là object, JSON Lines hoặc array object: {source_path}")

    records = [json.dumps(item, ensure_ascii=False) for item in root if isinstance(item, dict)]
    if not records:
        raise ValueError(f"JSON array không có object dữ liệu: {source_path}")

    # Infer the nested record schema with Spark, then parse/explode the original
    # file with Spark SQL. This handles DataMB's one-line top-level array.
    record_schema = spark.read.json(spark.sparkContext.parallelize(records)).schema
    array_schema = ArrayType(record_schema, containsNull=True)
    parsed = whole_file.select(
        F.from_json(F.col("value"), array_schema, {"mode": "FAILFAST"}).alias("_items")
    )
    return parsed.select(F.explode(F.col("_items")).alias("_record")).select("_record.*")


def publish_staging(spark: SparkSession, staging_root: str, silver_root: str, overwrite: bool) -> None:
    jvm = spark._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    target = jvm.org.apache.hadoop.fs.Path(silver_root)
    staging = jvm.org.apache.hadoop.fs.Path(staging_root)
    fs = target.getFileSystem(conf)
    backup = jvm.org.apache.hadoop.fs.Path(silver_root + ".__backup_" + uuid.uuid4().hex[:10])

    had_previous = fs.exists(target)
    if had_previous and not overwrite:
        raise FileExistsError(
            f"Silver đã tồn tại: {silver_root}. Dùng --overwrite để thay thế sau khi chạy thành công."
        )
    if had_previous and not fs.rename(target, backup):
        raise RuntimeError(f"Không thể chuyển Silver cũ sang bản dự phòng: {silver_root}")

    if not fs.rename(staging, target):
        if had_previous:
            fs.rename(backup, target)
        raise RuntimeError(f"Không thể công bố Silver mới từ vùng staging: {staging_root}")

    if had_previous:
        fs.delete(backup, True)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    root = hdfs_root()
    parser = argparse.ArgumentParser(
        description="Dùng Spark SQL chuẩn hóa dữ liệu Bronze thành các bộ Parquet trong Silver."
    )
    parser.add_argument("--bronze", default=f"{root}/bronze", help="Bronze HDFS root hoặc file:/// path.")
    parser.add_argument(
        "--silver",
        default=f"{root}/silver/normalized_sources",
        help="Silver output root; mỗi nguồn sẽ thành một thư mục .parquet tương ứng.",
    )
    parser.add_argument("--max-files", type=int, help="Chỉ chạy N nguồn đầu tiên; chủ yếu dùng để thử nhanh.")
    parser.add_argument("--dry-run", action="store_true", help="Liệt kê nguồn/đích và kiểm tra gzip, không ghi dữ liệu.")
    parser.add_argument("--overwrite", action="store_true", help="Thay Silver hiện tại sau khi toàn bộ lần chạy thành công.")
    args = parser.parse_args(argv)
    if args.max_files is not None and args.max_files < 1:
        parser.error("--max-files phải lớn hơn 0.")
    ensure_distinct_roots(args.bronze, args.silver)
    return args


def run_normalization(args: argparse.Namespace, spark: SparkSession) -> dict[str, Any]:
    sources, unsupported = list_source_files(spark, args.bronze)
    if unsupported:
        preview = ", ".join(unsupported[:10])
        suffix = f" (và {len(unsupported) - 10} tệp khác)" if len(unsupported) > 10 else ""
        raise ValueError(f"Có định dạng chưa hỗ trợ trong Bronze: {preview}{suffix}")
    if not sources:
        raise ValueError(f"Không tìm thấy CSV/CSV.GZ/JSON trong {args.bronze}")
    if args.max_files is not None:
        sources = sources[:args.max_files]

    verify_gzip_headers(spark, sources)
    if args.dry_run:
        print(f"[DRY RUN] {len(sources):,} tệp Bronze → {args.silver}", flush=True)
        for _, relative in sources:
            print(f"  {relative} -> {output_relative_path(relative)}", flush=True)
        return {"processed_files": 0, "row_count": 0}

    output_path = spark._jvm.org.apache.hadoop.fs.Path(args.silver)
    output_fs = output_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    if output_fs.exists(output_path) and not args.overwrite:
        raise FileExistsError(
            f"Silver đã tồn tại: {args.silver}. Dùng --overwrite để thay thế sau khi chạy thành công."
        )

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8]
    staging_root = args.silver.rstrip("/") + ".__staging_" + run_id
    started_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    manifest_items: list[dict[str, Any]] = []
    total_rows = 0

    try:
        for index, (source_path, relative) in enumerate(sources, start=1):
            extension = path_extension(relative)
            LOGGER.info("[%s/%s] Đọc %s", index, len(sources), relative)
            reader = spark.read.option("mode", "FAILFAST")
            if extension == ".json":
                frame = read_json_source(spark, source_path)
            else:
                frame = (
                    reader.option("header", "true")
                    .option("encoding", "UTF-8")
                    .option("multiLine", "true")
                    .option("nullValue", r"\N")
                    .csv(source_path)
                )

            flattened = flatten_source(frame)
            metadata = metadata_for(relative)
            normalized = normalized_sql(flattened, metadata).persist(StorageLevel.MEMORY_AND_DISK)
            row_count = normalized.count()
            relative_output = output_relative_path(relative)
            destination = staging_root.rstrip("/") + "/" + relative_output
            try:
                normalized.write.mode("errorifexists").parquet(destination)
            finally:
                normalized.unpersist()
            manifest_items.append({
                "source": relative,
                "output": relative_output,
                "source_system": metadata["source_system"],
                "source_table": metadata["source_table"],
                "source_season": metadata["source_season"],
                "source_team": metadata["source_team"],
                "row_count": row_count,
                "columns": normalized.columns,
                "schema": normalized.schema.jsonValue(),
                "column_mapping": {
                    original: (target + "_raw" if target in WAGE_FIELDS or target in EURO_AMOUNT_FIELDS else target)
                    for original, target in zip(frame.columns, flattened.columns, strict=True)
                } if extension != ".json" else {},
            })
            total_rows += row_count
            LOGGER.info("[%s/%s] Đã ghi %s dòng -> %s", index, len(sources), row_count, destination)

        manifest = {
            "run_id": run_id,
            "started_at_utc": started_at,
            "completed_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "bronze_root": args.bronze,
            "silver_root": args.silver,
            "file_count": len(manifest_items),
            "total_rows": total_rows,
            "format": "parquet",
            "compression": "snappy",
            "transform_rules": [
                "source headers -> lowercase snake_case; nested JSON structs flattened",
                "trim/collapse whitespace; blank, dash and common NA tokens -> NULL",
                "IDs remain strings; known metrics cast to BIGINT or DOUBLE; dates and booleans typed",
                "person/club labels keep display text and receive an unaccented *_key column",
                "wages and transfer-market amounts retain raw strings and expose parsed currency amounts",
            ],
            "files": manifest_items,
        }
        write_manifest(spark, staging_root.rstrip("/") + "/_metadata/normalization_manifest", manifest)
        publish_staging(spark, staging_root, args.silver, args.overwrite)
        return {"processed_files": len(manifest_items), "row_count": total_rows, "run_id": run_id}
    except Exception:
        staging_path = spark._jvm.org.apache.hadoop.fs.Path(staging_root)
        staging_fs = staging_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
        if staging_fs.exists(staging_path):
            staging_fs.delete(staging_path, True)
        raise


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    args = parse_args(argv)
    spark = (
        SparkSession.builder.appName("PremierLeagueBronzeToSilver")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        result = run_normalization(args, spark)
        if args.dry_run:
            print(f"[HOÀN TẤT] Đã kiểm tra danh sách nguồn trong {args.bronze}.", flush=True)
        else:
            print(
                f"[THÀNH CÔNG] {result['processed_files']:,} bộ Parquet, "
                f"{result['row_count']:,} dòng -> {args.silver}",
                flush=True,
            )
        return 0
    except Exception as error:
        LOGGER.exception("[LỖI] Chuẩn hóa Bronze → Silver thất bại: %s", error)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
