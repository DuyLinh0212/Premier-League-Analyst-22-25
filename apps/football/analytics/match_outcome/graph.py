"""Parameter-only Neo4j Query API publication with idempotent event keys."""
import base64
import csv
import json
import os
import shutil
import urllib.request
from pathlib import Path

def query(statement, parameters=None):
    uri = os.getenv("NEO4J_HTTP_URI", "http://neo4j:7474").rstrip("/")
    credentials = (os.getenv("NEO4J_USER", "neo4j") + ":" + os.environ["NEO4J_PASSWORD"]).encode()
    database = os.getenv("NEO4J_DATABASE", "neo4j")
    request = urllib.request.Request(uri + "/db/" + database + "/query/v2", data=json.dumps({"statement": statement, "parameters": parameters or {}}, allow_nan=False).encode(),
        headers={"Content-Type": "application/json", "Accept": "application/json", "Authorization": "Basic " + base64.b64encode(credentials).decode()})
    with urllib.request.urlopen(request, timeout=60) as response:
        result = json.loads(response.read())
    if result.get("errors"):
        raise RuntimeError("Neo4j: " + json.dumps(result["errors"]))
    data = result.get("data", {})
    return [dict(zip(data.get("fields", []), row)) for row in data.get("values", [])]

def schema():
    for label, key in [("AnalysisRun", "analysis_run_id"), ("Club", "club_id"), ("Match", "match_id"),
                       ("ModelVersion", "model_version"), ("MatchPrediction", "key"),
                       ("KNNPrediction", "key"), ("KNNNeighbor", "key")]:
        query(f"CREATE CONSTRAINT {label.lower()}_key IF NOT EXISTS FOR (n:{label}) REQUIRE n.{key} IS UNIQUE")

def publish_predictions(items, run_id=None):
    # A single transaction makes each chunk and its entity links visible together.
    for start in range(0, len(items), 100):
        chunk = []
        for event, prediction in items[start:start+100]:
            chunk.append({"event_id": event["event_id"], "match_id": event.get("match_id", event["event_id"]),
                          "home_team": event["home_team"], "away_team": event["away_team"],
                          "home_name": event.get("home_name", event["home_team"]), "away_name": event.get("away_name", event["away_team"]),
                          "season": event.get("season"), "kind": event["kind"], "source": event.get("source", "manual"),
                          "match_date": event.get('match_date'),
                          "statistics_json": json.dumps({side:event[side+'_stats'] for side in ('home','away')},ensure_ascii=False,allow_nan=False),
                          "model_version": prediction["model_version"], **prediction["probabilities"],
                          'request_json':json.dumps(event,ensure_ascii=False,allow_nan=False),
                          'result_json':json.dumps(prediction,ensure_ascii=False,allow_nan=False),
                          "imputed_fields": prediction["imputed_fields"], "run_id": run_id or "kafka_" + event["event_id"]})
        query("""UNWIND $rows AS row
          MERGE (run:AnalysisRun {analysis_run_id:row.run_id}) ON CREATE SET run.status='processing',run.project='football-v3'
          MERGE (model:ModelVersion {model_version:row.model_version})
          MERGE (home:Club {club_id:row.home_team}) SET home.club_name=row.home_name
          MERGE (away:Club {club_id:row.away_team}) SET away.club_name=row.away_name
          MERGE (match:Match {match_id:row.match_id}) SET match.season=row.season,match.kind=row.kind,match.match_date=row.match_date
          MERGE (prediction:MatchPrediction {key:row.event_id+'_'+row.model_version}) SET prediction += row
          MERGE (run)-[:PRODUCED]->(prediction) MERGE (prediction)-[:FOR_MATCH]->(match)
          MERGE (prediction)-[:USING_MODEL]->(model) MERGE (match)-[:HOME_TEAM]->(home) MERGE (match)-[:AWAY_TEAM]->(away)
        """, {"rows": chunk})
        if run_id is None:
            query("UNWIND $ids AS id MATCH (r:AnalysisRun {analysis_run_id:id}) SET r.status='complete'", {"ids": list({r["run_id"] for r in chunk})})

def publish_bundle(output, knn, run_id="football_v3_seed"):
    from .inference import load_artifact, predict
    output, knn = Path(output), Path(knn)
    artifact = load_artifact(output)
    rows = [json.loads(line) for line in (output / "matches.jsonl").read_text(encoding="utf-8").splitlines()]
    schema()
    query("MERGE (r:AnalysisRun {analysis_run_id:$id}) SET r.status='processing',r.project='football-v3'", {"id": run_id})
    publish_predictions([(r, predict(r, artifact)) for r in rows], run_id)
    for filename, label in [("knn_predictions.csv", "KNNPrediction"), ("knn_neighbors.csv", "KNNNeighbor")]:
        with (knn / filename).open(encoding="utf-8-sig", newline="") as stream:
            data = list(csv.DictReader(stream))
        for index, row in enumerate(data):
            row["key"] = f"{run_id}_{filename}_{index}"
            row["run_id"] = run_id
        query(f"UNWIND $rows AS row MERGE (n:{label} {{key:row.key}}) SET n += row WITH n,row MATCH (r:AnalysisRun {{analysis_run_id:row.run_id}}) MERGE (r)-[:PRODUCED]->(n)", {"rows": data})
        query(f"""UNWIND $rows AS row MATCH (n:{label} {{key:row.key}})
          MERGE (club:Club {{club_id:row.query_club_id}}) SET club.club_name=row.query_club_name
          MERGE (n)-[:FOR_CLUB]->(club)""", {"rows":data})
        if label == 'KNNNeighbor':
            query("""UNWIND $rows AS row MATCH (n:KNNNeighbor {key:row.key})
              MERGE (club:Club {club_id:row.neighbor_club_id}) SET club.club_name=row.neighbor_club_name
              MERGE (n)-[:NEIGHBOR_CLUB]->(club)""", {"rows":data})
    query("MATCH (m:ModelVersion {model_version:$version}) SET m.metrics_json=$metrics,m.description=$description", {
        "version": artifact["model_version"], "metrics": (output / "metrics.json").read_text(encoding="utf-8"), "description": artifact["description"]})
    counts = query("MATCH (r:AnalysisRun {analysis_run_id:$id})-[:PRODUCED]->(n) RETURN labels(n)[0] AS kind,count(n) AS count", {"id": run_id})
    observed = {r["kind"]: r["count"] for r in counts}
    expected = {"MatchPrediction": 1140, "KNNPrediction": 20, "KNNNeighbor": 100}
    if observed != expected:
        raise RuntimeError(f"Publication count mismatch: {observed}, expected {expected}")
    query("MATCH (r:AnalysisRun {analysis_run_id:$id}) SET r.status='complete',r.completed_at=toString(datetime())", {"id": run_id})
    version = output / "versions" / artifact["model_version"]
    version.mkdir(parents=True, exist_ok=True)
    for name in ["model.json", "metrics.json", "matches.jsonl"]:
        shutil.copy2(output / name, version / name)
    shutil.copytree(knn, version / "knn", dirs_exist_ok=True)
    pointer = output / "published.json.tmp"
    pointer.write_text(json.dumps({"model_version": artifact["model_version"], "run_id": run_id}), encoding="utf-8")
    pointer.replace(output / "published.json")
    return observed
