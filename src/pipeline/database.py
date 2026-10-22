"""Versioned PostgreSQL publication; a complete schema is the immutable snapshot."""
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import date, datetime
import json
import logging
from pathlib import Path
import re

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
import sqlglot
from sqlglot import exp

from .config import Config

LOG = logging.getLogger(__name__)
REQUEST = ContextVar("plca_database_request", default=None)


def provenance():
    request = REQUEST.get()
    return {"run_id": request["run"]["run_id"], "source": "postgresql"} if request else {}


def connect(config=None, role="writer"):
    return psycopg.connect(**(config or Config()).connection(role), row_factory=dict_row)


def initialize(config=None):
    with connect(config) as db:
        db.execute("CREATE SCHEMA IF NOT EXISTS plca")
        db.execute("""CREATE TABLE IF NOT EXISTS plca.runs (
            run_id text PRIMARY KEY, schema_name text UNIQUE NOT NULL, report_date date NOT NULL,
            status text NOT NULL, manifest jsonb NOT NULL, artifact_path text NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now())""")
        db.execute("CREATE TABLE IF NOT EXISTS plca.active (id integer PRIMARY KEY CHECK(id=1), run_id text REFERENCES plca.runs(run_id))")
        db.execute("GRANT USAGE ON SCHEMA plca TO plca_reader")
        db.execute("GRANT SELECT ON ALL TABLES IN SCHEMA plca TO plca_reader")


def postgres_type(field):
    kind = field["type"]
    if isinstance(kind, dict):
        return "jsonb"
    return {"string": "text", "integer": "integer", "long": "bigint", "short": "integer",
            "byte": "integer", "double": "double precision", "float": "double precision",
            "date": "date", "timestamp": "timestamptz", "timestamp_ntz": "timestamp",
            "boolean": "boolean"}.get(kind, "numeric" if kind.startswith("decimal(") else "text")


def load_snapshot(ctx, spark):
    initialize(ctx.config)
    with connect(ctx.config) as db:
        db.execute("SELECT pg_advisory_xact_lock(hashtext('plca-publication'))")
        old = db.execute("SELECT status FROM plca.runs WHERE run_id=%s", (ctx.run_id,)).fetchone()
        if old and old["status"] == "published":
            LOG.info("Snapshot %s đã công bố; giữ nguyên dữ liệu PostgreSQL.", ctx.run_id)
            return
        schema = sql.Identifier(ctx.schema_name)
        db.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(schema))
        db.execute(sql.SQL("CREATE SCHEMA {}").format(schema))
        for key, entry in ctx.manifest["tables"].items():
            name = key.split("/")[-1]
            fields = entry["schema"]["fields"]
            columns = [f["name"] for f in fields]
            definitions = [sql.SQL("{} {}").format(sql.Identifier(f["name"]), sql.SQL(postgres_type(f))) for f in fields]
            db.execute(sql.SQL("CREATE TABLE {}.{} ({})").format(schema, sql.Identifier(name), sql.SQL(",").join(definitions)))
            frame = spark.read.parquet(entry["path"]).select(*columns)
            copy = sql.SQL("COPY {}.{} ({}) FROM STDIN").format(schema, sql.Identifier(name), sql.SQL(",").join(map(sql.Identifier, columns)))
            with db.cursor().copy(copy) as target:
                for row in frame.toLocalIterator():
                    target.write_row(tuple(Jsonb(value) if isinstance(value, (list, dict)) else value for value in row))
            count = db.execute(sql.SQL("SELECT count(*) AS n FROM {}.{}").format(schema, sql.Identifier(name))).fetchone()["n"]
            if count != entry["row_count"]:
                raise RuntimeError(f"Số dòng PostgreSQL không khớp HDFS: {name}")
            if entry["keys"]:
                db.execute(sql.SQL("CREATE UNIQUE INDEX ON {}.{} ({})").format(
                    schema, sql.Identifier(name), sql.SQL(",").join(map(sql.Identifier, entry["keys"]))))
            if "season" in columns:
                db.execute(sql.SQL("CREATE INDEX ON {}.{} (season)").format(schema, sql.Identifier(name)))
            db.execute(sql.SQL("ANALYZE {}.{}").format(schema, sql.Identifier(name)))
            LOG.info("Đã nạp PostgreSQL: %s, %d dòng.", name, count)
        db.execute(sql.SQL("CREATE VIEW {}.club_names AS SELECT club_id,min(club_name) AS club_name FROM {}.dim_club GROUP BY club_id").format(schema, schema))
        db.execute(sql.SQL("CREATE TABLE {}.analysis_results (analysis_id text PRIMARY KEY, payload jsonb NOT NULL)").format(schema))
        index = json.loads((ctx.local / "analysis/index.json").read_text())
        for record in index:
            payload = json.loads((ctx.local / record["file"]).read_text())
            db.execute(sql.SQL("INSERT INTO {}.analysis_results VALUES (%s,%s)").format(schema), (record["id"], Jsonb(payload)))
        db.execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO plca_reader").format(schema))
        db.execute(sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO plca_reader").format(schema))
        db.execute("""INSERT INTO plca.runs(run_id,schema_name,report_date,status,manifest,artifact_path)
            VALUES(%s,%s,%s,'staged',%s,%s) ON CONFLICT(run_id) DO UPDATE SET
            status=excluded.status,manifest=excluded.manifest,artifact_path=excluded.artifact_path""",
                   (ctx.run_id, ctx.schema_name, ctx.manifest["report_date"], Jsonb(ctx.manifest), str(ctx.local)))


def publish(ctx):
    with connect(ctx.config) as db:
        db.execute("SELECT pg_advisory_xact_lock(hashtext('plca-publication'))")
        run = db.execute("SELECT * FROM plca.runs WHERE run_id=%s FOR UPDATE", (ctx.run_id,)).fetchone()
        if not run or run["status"] not in ("staged", "published"):
            raise RuntimeError("Chưa có snapshot PostgreSQL hoàn chỉnh")
        previous = db.execute("SELECT run_id FROM plca.active WHERE id=1").fetchone()
        ctx.manifest.setdefault("previous_run_id", previous["run_id"] if previous else None)
        db.execute("UPDATE plca.runs SET status='published',manifest=%s WHERE run_id=%s", (Jsonb(ctx.manifest), ctx.run_id))
        db.execute("INSERT INTO plca.active VALUES(1,%s) ON CONFLICT(id) DO UPDATE SET run_id=excluded.run_id", (ctx.run_id,))
    ctx.save()


def rollback(ctx):
    previous = ctx.manifest.get("previous_run_id")
    ctx.manifest["rollback_required"] = True
    ctx.save()
    with connect(ctx.config) as db:
        db.execute("UPDATE plca.active SET run_id=%s WHERE id=1 AND run_id=%s", (previous, ctx.run_id))
        db.execute("UPDATE plca.runs SET status='rejected' WHERE run_id=%s", (ctx.run_id,))
    ctx.manifest["rollback_required"] = False
    ctx.save()
    LOG.error("Đã khôi phục snapshot Web về %s sau lỗi nghiệm thu.", previous)


def recover_rollbacks(config):
    from .context import Context
    for path in (config.state / "runs").glob("*/manifest.json"):
        manifest = json.loads(path.read_text())
        if manifest.get("rollback_required"):
            rollback(Context(manifest["run_id"], config))


def finalize_manifest(ctx):
    with connect(ctx.config) as db:
        db.execute("UPDATE plca.runs SET manifest=%s WHERE run_id=%s", (Jsonb(ctx.manifest), ctx.run_id))


class Row(dict):
    def asDict(self, recursive=False):
        return dict(self)


class Result:
    def __init__(self, rows):
        self.rows = [Row(row) for row in rows]

    def collect(self):
        return self.rows

    def first(self):
        return self.rows[0] if self.rows else None


def postgres_sql(query):
    # Names are constant within each club/player group, so MIN is deterministic
    # and matches Spark FIRST(..., true) without relying on a PostgreSQL extension.
    query = re.sub(r"\bfirst\(\s*([\w.]+)\s*,\s*true\s*\)", r"min(\1)", query, flags=re.I)
    tree = sqlglot.parse_one(query, read="spark")
    for expression in list(tree.find_all(exp.RegexpExtract)):
        group = expression.args.get("group") or exp.Literal.number(0)
        # PostgreSQL 18 has regexp_substr with an explicit capture-group argument.
        # NULL on no match is intentional for matchday's NULLIF + integer cast.
        expression.replace(exp.Anonymous(this="regexp_substr", expressions=[
            expression.this.copy(), expression.expression.copy(), exp.Literal.number(1),
            exp.Literal.number(1), exp.Literal.string(""), group.copy()]))
    names = tree.named_selects
    return tree.sql(dialect="postgres"), {name.lower(): name for name in names if name != "*"}


class PostgresRuntime:
    def __init__(self, config=None):
        self.config = config or Config()

    def start(self):
        return self

    def stop(self):
        pass

    @contextmanager
    def request(self, run_id=None, allow_staged=False):
        with connect(self.config, "reader") as db:
            db.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            if run_id:
                run = db.execute("SELECT * FROM plca.runs WHERE run_id=%s", (run_id,)).fetchone()
            else:
                run = db.execute("SELECT r.* FROM plca.runs r JOIN plca.active a ON r.run_id=a.run_id WHERE a.id=1").fetchone()
            if not run or run["status"] not in (("published", "staged") if allow_staged else ("published",)):
                raise RuntimeError("Chưa có snapshot phân tích đã công bố")
            db.execute(sql.SQL("SET LOCAL search_path TO {},public").format(sql.Identifier(run["schema_name"])))
            token = REQUEST.set({"db": db, "run": run})
            try:
                yield self
            finally:
                REQUEST.reset(token)

    @property
    def manifest(self):
        value = REQUEST.get()
        return {**value["run"]["manifest"], "run_id": value["run"]["run_id"], "dataset_id": value["run"]["run_id"]} if value else {}

    @property
    def reports_dir(self):
        return Path(REQUEST.get()["run"]["artifact_path"]) / "reports"

    def sql(self, query):
        current = REQUEST.get()
        if current is None:
            raise RuntimeError("Truy vấn phải nằm trong transaction snapshot của request")
        statement, names = postgres_sql(query)
        records = current["db"].execute(statement).fetchall()
        return Result([{names.get(key, key): value for key, value in row.items()} for row in records])

    def report(self, season, slug):
        path = self.reports_dir / season / (slug + ".json")
        if not path.is_file():
            raise RuntimeError("Báo cáo chưa được xuất cho snapshot này")
        exported = json.loads(path.read_text())["report"]
        analysis = ("head-to-head-matrix" if slug == "head-to-head" else slug) + "__" + season
        record = REQUEST.get()["db"].execute("SELECT payload FROM analysis_results WHERE analysis_id=%s", (analysis,)).fetchone()
        if record is None:
            raise RuntimeError("PostgreSQL chưa có dữ liệu của báo cáo này")
        value = {**record["payload"], "title": exported["title"]}
        value["meta"] = {**value.get("meta", {}), "season": season, "dataset_id": self.manifest["run_id"], **provenance()}
        value["image_url"] = f"/reports/{season}/{slug}.png?run_id={self.manifest['run_id']}"
        return value
