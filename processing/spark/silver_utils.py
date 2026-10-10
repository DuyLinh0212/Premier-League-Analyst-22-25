# silver_utils.py
import re
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, trim, lower, when, to_date

def get_spark_session(app_name="PL_Silver_Standardization"):
    """Khởi tạo SparkSession kết nối tới HDFS nội bộ"""
    return SparkSession.builder \
        .appName(app_name) \
        .master(os.getenv("SPARK_MASTER", "local[2]")) \
        .config("spark.hadoop.fs.defaultFS", os.getenv("HDFS_URI", "hdfs://localhost:9000")) \
        .config("spark.hadoop.dfs.blocksize", "67108864").config("spark.hadoop.parquet.block.size", "67108864") \
        .config("spark.hadoop.dfs.replication", "2") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()

def to_snake_case(text: str) -> str:
    """Chuẩn hóa tên cột: bỏ %, khoảng trắng, gạch ngang -> snake_case"""
    text = text.replace("%", "_pct")
    text = re.sub(r'[/\\-]+', '_', text)
    text = re.sub(r'\s+', '_', text)
    text = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', text)
    text = re.sub(r'_+', '_', text)
    return text.strip('_').lower()

def clean_column_names(df, prefix="", suffix=""):
    """Đổi tên toàn bộ cột của DataFrame sang snake_case, hỗ trợ thêm prefix/suffix"""
    for col_name in df.columns:
        new_name = to_snake_case(col_name)
        if prefix:
            new_name = f"{prefix}_{new_name}"
        if suffix:
            new_name = f"{new_name}_{suffix}"
        df = df.withColumnRenamed(col_name, new_name)
    return df

# Bảng từ điển Mapping tên chuẩn CLB (FBref vs Transfermarkt)
CLUB_MAPPING = {
    "arsenal": "ARS",
    "aston villa": "AVL",
    "bournemouth": "BOU",
    "brentford": "BRE",
    "brighton": "BHA",
    "brighton and hove albion": "BHA",
    "burnley": "BUR",
    "chelsea": "CHE",
    "crystal palace": "CRY",
    "everton": "EVE",
    "fulham": "FUL",
    "ipswich town": "IPS",
    "leeds united": "LEE",
    "leicester city": "LEI",
    "liverpool": "LIV",
    "luton town": "LUT",
    "manchester city": "MCI",
    "manchester united": "MUN",
    "manchester utd": "MUN",
    "newcastle united": "NEW",
    "newcastle utd": "NEW",
    "nottingham forest": "NFO",
    "sheffield united": "SHU",
    "southampton": "SOU",
    "tottenham": "TOT",
    "tottenham hotspur": "TOT",
    "west ham": "WHU",
    "west ham united": "WHU",
    "wolverhampton wanderers": "WOL",
    "wolves": "WOL"
}

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def get_club_id(name):
    if not name:
        return None
    cleaned = re.sub(r'[-_]+', ' ', str(name).strip().lower())
    cleaned = re.sub(r'\s+', ' ', cleaned)
    if cleaned in CLUB_MAPPING:
        return CLUB_MAPPING[cleaned]
    for k, v in CLUB_MAPPING.items():
        if k == cleaned or k in cleaned or cleaned in k:
            return v
    return cleaned.upper()[:3]

