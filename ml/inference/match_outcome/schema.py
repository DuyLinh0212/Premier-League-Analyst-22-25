"""One feature contract shared by training, API, Kafka and UI."""
import math

SCHEMA_VERSION = 1
LABELS = ("home_win", "draw", "away_win")
REQUIRED = ("possession", "shots", "shots_on_target")
METRICS = {
    "possession": ("Kiểm soát bóng", "%", 100),
    "shots": ("Tổng số cú sút", "lần", None),
    "shots_on_target": ("Sút trúng đích", "lần", None),
    "shot_accuracy": ("Độ chính xác sút", "%", 100),
    "penalties_attempted": ("Penalty thực hiện", "lần", None),
    "yellow_cards": ("Thẻ vàng", "thẻ", None),
    "red_cards": ("Thẻ đỏ", "thẻ", None),
    "second_yellow_cards": ("Thẻ vàng thứ hai", "thẻ", None),
    "fouls_committed": ("Phạm lỗi", "lần", None),
    "fouls_drawn": ("Bị phạm lỗi", "lần", None),
    "offsides": ("Việt vị", "lần", None),
    "crosses": ("Tạt bóng", "lần", None),
    "interceptions": ("Đánh chặn", "lần", None),
    "tackles_won": ("Tắc bóng thành công", "lần", None),
    "penalties_won": ("Penalty được hưởng", "lần", None),
    "penalties_conceded": ("Penalty phải chịu", "lần", None),
}
SOURCE_METRICS = {
    "score_fixtures": {"Poss": "possession"},
    "shooting_for": {"Sh": "shots", "SoT": "shots_on_target", "SoT%": "shot_accuracy", "PKatt": "penalties_attempted"},
    "miscellaneous_for": {"CrdY": "yellow_cards", "CrdR": "red_cards", "2CrdY": "second_yellow_cards",
                          "Fls": "fouls_committed", "Fld": "fouls_drawn", "Off": "offsides", "Crs": "crosses",
                          "Int": "interceptions", "TklW": "tackles_won", "PKwon": "penalties_won", "PKcon": "penalties_conceded"},
}
LEAKAGE_HEADERS = {"Result", "GF_match", "GA_match", "Gls", "G/Sh", "G/SoT", "PK", "GA_gk", "Saves", "Save%", "CS", "PKA", "PKsv", "PKm", "OG", "Score", "Scorer", "Assist", "Minute"}

def numeric(value):
    if value is None or str(value).strip() in {"", "NA", "N/A"}:
        return None
    result = float(str(value).replace(",", "").replace("%", ""))
    if not math.isfinite(result):
        raise ValueError("Số liệu phải hữu hạn")
    return result

def validate_stats(stats, require=True):
    unknown = set(stats) - set(METRICS) - {"formation"}
    if unknown:
        raise ValueError("Tham số không hỗ trợ: " + ", ".join(sorted(unknown)))
    result = {}
    for key, (_, unit, upper) in METRICS.items():
        value = numeric(stats.get(key))
        if value is None and require and key in REQUIRED:
            raise ValueError(f"Thiếu {key}")
        if value is not None and (value < 0 or value > (upper if upper is not None else 1000)):
            raise ValueError(f"{key} ngoài khoảng hợp lệ")
        if value is not None and unit != "%" and value != int(value):
            raise ValueError(f"{key} phải là số nguyên")
        result[key] = value
    if result["shots"] is not None and result["shots_on_target"] is not None:
        if result["shots_on_target"] > result["shots"]:
            raise ValueError("Sút trúng đích không được vượt tổng số sút")
        if result["shot_accuracy"] is None:
            result["shot_accuracy"] = 100 * result["shots_on_target"] / result["shots"] if result["shots"] else 0.0
    formation = stats.get("formation")
    if formation is not None and (not isinstance(formation, str) or len(formation) > 30):
        raise ValueError("Đội hình không hợp lệ")
    result["formation"] = formation or None
    return result

def feature_schema(artifact=None):
    disabled = (artifact or {}).get("disabled_metrics", [])
    return {"schema_version": SCHEMA_VERSION, "labels": list(LABELS), "required": list(REQUIRED),
            "description": "Ước tính từ thống kê tổng kết trận, chưa phải dự đoán theo phút.",
            "metrics": [{"key": k, "label": v[0], "unit": v[1], "max": v[2] if v[2] is not None else 1000, "required": k in REQUIRED,
                         "used_by_model": k not in disabled} for k, v in METRICS.items()],
            "formation": True, "unavailable": ["passes", "pass_accuracy", "minute_snapshots"]}
