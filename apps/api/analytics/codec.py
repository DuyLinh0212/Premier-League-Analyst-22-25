"""Shared serialization and naming helpers; persistence lives only in Neon."""
import hashlib
import json
import os
from datetime import datetime, timezone


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False, separators=(",", ":"))


def digest(value):
    return hashlib.sha256(encode(value).encode("utf-8")).hexdigest()


def now():
    return datetime.now(timezone.utc).isoformat()


def namespace():
    return os.getenv("APP_NAMESPACE", "premier-league-22-25")
