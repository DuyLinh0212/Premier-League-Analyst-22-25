"""Neo4j serving layer: immutable datasets/reports, atomic publication pointers."""
import hashlib
import json
import os
from datetime import datetime, timezone
from match_outcome.graph import query
from . import VERSION


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False, separators=(",", ":"))


def digest(value):
    return hashlib.sha256(encode(value).encode()).hexdigest()


def now():
    return datetime.now(timezone.utc).isoformat()


def namespace():
    return os.getenv("APP_NAMESPACE", "premier-league-22-25")


def schema():
    for label in ("PLDataset", "PLTable", "PLRecord", "PLRun", "PLReport", "PLActive", "PLEvent", "PLJob"):
        query(f"CREATE CONSTRAINT {label.lower()}_key IF NOT EXISTS FOR (n:{label}) REQUIRE n.key IS UNIQUE")
    query("CREATE INDEX plreport_lookup IF NOT EXISTS FOR (n:PLReport) ON (n.namespace,n.kind)")
    query("CREATE INDEX plrecord_table IF NOT EXISTS FOR (n:PLRecord) ON (n.table_key)")
    query("CREATE INDEX plrecord_page IF NOT EXISTS FOR (n:PLRecord) ON (n.table_key,n.ordinal)")


def begin_dataset(dataset_id, metadata):
    key = namespace() + ":" + dataset_id
    query("MERGE (d:PLDataset {key:$key}) ON CREATE SET d.status='loading',d.created_at=$now SET d.namespace=$ns,d.metadata_json=$meta",
          {"key": key, "ns": namespace(), "meta": encode(metadata), "now": now()})
    return key


def import_table(dataset_key, name, rows, metadata):
    table_key = dataset_key + ":" + name
    expected_count = metadata.get('row_count')
    if expected_count is None:
        expected_count = len(rows)
    existing = query("MATCH (t:PLTable {key:$key}) RETURN t.status AS status,t.checksum AS checksum,t.row_count AS row_count", {"key": table_key})
    if existing and existing[0]["status"] == "complete":
        if existing[0]["checksum"] != metadata["checksum"] or existing[0]["row_count"] != expected_count:
            raise ValueError("Immutable table conflict: " + name)
        return expected_count
    keys = metadata.get("keys", [])
    prepared, seen = [], set()
    # Large raw Transfermarkt tables contain millions of rows.  Keep the
    # write size configurable so local Neo4j and CI can trade request count
    # against transaction memory without changing the immutable table model.
    try:
        batch_size = max(100, int(os.getenv("NEO4J_IMPORT_BATCH_SIZE", "20000")))
    except ValueError:
        batch_size = 20000
    query("MATCH (d:PLDataset {key:$dataset}) MERGE (t:PLTable {key:$key}) SET t.namespace=$ns,t.name=$name,t.metadata_json=$meta,t.checksum=$sha,t.status='loading' MERGE (d)-[:HAS_TABLE]->(t)",
          {"dataset": dataset_key, "key": table_key, "ns": namespace(), "name": name, "meta": encode(metadata), "sha": metadata["checksum"]})
    def flush():
        query("MATCH (t:PLTable {key:$table}) UNWIND $rows AS row MERGE (n:PLRecord {key:row.key}) SET n.table_key=$table,n.payload_json=row.payload,n.ordinal=row.ordinal MERGE (t)-[:HAS_RECORD]->(n)",
              {"table": table_key, "rows": prepared})
        prepared.clear()
    observed_count = 0
    for index, row in enumerate(rows):
        identity = [row.get(k) for k in keys] if keys else [index]
        record_key = table_key + ":" + digest(identity)
        if keys and name[:4] != 'raw/' and record_key in seen:
            raise ValueError("Duplicate table key: " + name)
        if keys and name[:4] != 'raw/':
            seen.add(record_key)
        prepared.append({"key": record_key, "payload": encode(row), "ordinal": index})
        observed_count += 1
        if len(prepared) >= batch_size:
            flush()
    if prepared:
        flush()
    if observed_count != expected_count:
        raise RuntimeError(f'Source count mismatch {name}: {observed_count} != {expected_count}')
    count = query("MATCH (t:PLTable {key:$key})-[:HAS_RECORD]->(n) RETURN count(n) AS count", {"key": table_key})[0]["count"]
    if count != expected_count:
        raise RuntimeError(f"Import count mismatch {name}: {count} != {expected_count}")
    query("MATCH (t:PLTable {key:$key}) SET t.status='complete',t.row_count=$count", {"key": table_key, "count": count})
    return count


def finish_dataset(dataset_key, expected_tables):
    observed = query("MATCH (d:PLDataset {key:$key})-[:HAS_TABLE]->(t) RETURN t.name AS name,t.status AS status", {"key": dataset_key})
    if len(observed) != expected_tables or any(x["status"] != "complete" for x in observed):
        raise RuntimeError("Incomplete dataset; publication blocked")
    query("MATCH (d:PLDataset {key:$dataset}) SET d.status='complete',d.completed_at=$now MERGE (p:PLActive {key:$pointer}) SET p.dataset_key=d.key",
          {"dataset": dataset_key, "pointer": namespace() + ":dataset", "now": now()})


def table_rows(name, dataset_key=None, offset=0, limit=None):
    if dataset_key is None:
        found = query("MATCH (p:PLActive {key:$key}) RETURN p.dataset_key AS key", {"key": namespace() + ":dataset"})
        if not found:
            raise LookupError("Chưa có dataset được công bố")
        dataset_key = found[0]["key"]
    key = dataset_key + ":" + name
    metadata = query("MATCH (t:PLTable {key:$key,status:'complete'}) RETURN t.row_count AS count", {"key": key})
    if not metadata:
        raise LookupError("Chưa có bảng " + name)
    result = []
    end = metadata[0]['count'] if limit is None else min(metadata[0]['count'], offset + limit)
    for start in range(offset, end, 1000):
        batch = query("MATCH (n:PLRecord {table_key:$key}) WHERE n.ordinal >= $offset AND n.ordinal < $end RETURN n.payload_json AS payload ORDER BY n.ordinal LIMIT 1000", {"key": key, "offset": start, 'end': min(end,start+1000)})
        result.extend(json.loads(x["payload"]) for x in batch)
    return result


def begin_run(run_id, dataset_key, parameters=None, simulated=False):
    key = namespace() + ":" + run_id
    query("MERGE (r:PLRun {key:$key}) ON CREATE SET r.status='processing',r.created_at=$now SET r.namespace=$ns,r.run_id=$id,r.dataset_key=$dataset,r.version=$version,r.parameters_json=$params,r.is_simulated=$sim",
          {"key": key, "ns": namespace(), "id": run_id, "dataset": dataset_key, "version": VERSION,
           "params": encode(parameters or {}), "now": now(), "sim": simulated})
    return key


def save_report(run_key, kind, payload, filters=None):
    filters = filters or {}
    key = run_key + ":" + kind + ":" + digest(filters)
    existing = query('MATCH (r:PLRun {key:$run,status:"complete"}) OPTIONAL MATCH (r)-[:PRODUCED]->(p:PLReport {key:$key}) RETURN p.payload_json AS payload',{'run':run_key,'key':key})
    if existing:
        if existing[0]['payload'] != encode(payload):
            raise ValueError('Completed reports are immutable')
        return key
    query("MATCH (r:PLRun {key:$run}) MERGE (p:PLReport {key:$key}) SET p.namespace=$ns,p.kind=$kind,p.filters_json=$filters,p.payload_json=$payload,p.created_at=$now,p.model_version=$model MERGE (r)-[:PRODUCED]->(p)",
          {"run": run_key, "key": key, "ns": namespace(), "kind": kind, "filters": encode(filters), "payload": encode(payload), "now": now(), 'model':payload.get('model_version') if isinstance(payload,dict) else None})
    return key


def finish_run(run_key, expected_reports):
    # Pointer and run status commit in ONE transaction. Old reports remain visible on failure.
    rows = query("MATCH (r:PLRun {key:$run})-[:PRODUCED]->(p) WITH r,collect(p) AS reports WHERE size(reports)=$count SET r.status='complete',r.completed_at=$now WITH r,reports UNWIND reports AS report MERGE (a:PLActive {key:$ns+':report:'+report.kind+':'+report.filters_json}) SET a.report_key=report.key RETURN count(report) AS count",
                 {"run": run_key, "count": expected_reports, "now": now(), "ns": namespace()})
    if not rows or rows[0]["count"] != expected_reports:
        raise RuntimeError("Incomplete report run; publication blocked")


def fail_run(run_key, error):
    query("MATCH (r:PLRun {key:$key}) WHERE r.status<>'complete' SET r.status='failed',r.error=$error", {"key": run_key, "error": str(error)[:1000]})


def report(kind, filters=None, run_id=None):
    params = {"kind": kind, "filters": encode(filters or {}), "ns": namespace()}
    if run_id:
        params["run"] = namespace() + ":" + run_id
        statement = "MATCH (r:PLRun {key:$run,status:'complete'})-[:PRODUCED]->(p:PLReport) WHERE p.kind=$kind AND p.filters_json=$filters"
    else:
        params["pointer"] = namespace() + ":report:" + kind + ":" + params["filters"]
        statement = "MATCH (a:PLActive {key:$pointer}),(p:PLReport {key:a.report_key})< -[:PRODUCED]-(r:PLRun {status:'complete'})".replace("< -", "<-")
    rows = query(statement + " RETURN p.payload_json AS payload,r.run_id AS run_id,r.version AS version,r.completed_at AS completed_at,r.is_simulated AS simulated", params)
    if not rows:
        raise LookupError("Chưa có báo cáo được công bố: " + kind)
    payload = json.loads(rows[0].pop("payload"))
    return payload, rows[0]


def runs(limit=50):
    return query("MATCH (r:PLRun {namespace:$ns}) RETURN r.run_id AS run_id,r.status AS status,r.version AS version,r.created_at AS created_at,r.completed_at AS completed_at,r.is_simulated AS is_simulated,r.error AS error,r.parameters_json AS parameters ORDER BY r.created_at DESC LIMIT $limit", {"ns": namespace(), "limit": limit})

def report_by_model(kind, model_version):
    rows=query('MATCH (r:PLRun {namespace:$ns,status:"complete"})-[:PRODUCED]->(p:PLReport {kind:$kind,model_version:$model}) RETURN p.payload_json AS payload ORDER BY r.completed_at DESC LIMIT 1',
               {'ns':namespace(),'kind':kind,'model':model_version})
    if not rows:
        raise LookupError('Không có phiên bản model đã công bố: '+model_version)
    return json.loads(rows[0]['payload'])


def catalog():
    return query("MATCH (p:PLActive {key:$key}),(d:PLDataset {key:p.dataset_key})-[:HAS_TABLE]->(t) RETURN t.name AS name,t.row_count AS row_count,t.metadata_json AS metadata ORDER BY t.name", {"key": namespace() + ":dataset"})
