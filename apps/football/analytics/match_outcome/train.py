"""Fit Spark multinomial LR with chronological tuning/calibration/test splits."""
import argparse
import csv
import hashlib
import json
import os
from pathlib import Path

import numpy as np
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession

from .dataset import write_dataset
from .inference import vector, softmax, predict
from .schema import LABELS, METRICS

def metrics(y, p):
    y, p = np.asarray(y, dtype=int), np.asarray(p, dtype=float)
    predicted = p.argmax(axis=1)
    confusion = np.zeros((3, 3), dtype=int)
    for actual, guess in zip(y, predicted):
        confusion[actual, guess] += 1
    f1 = [2 * confusion[i, i] / max(1, confusion[i].sum() + confusion[:, i].sum()) for i in range(3)]
    return {"samples": len(y), "accuracy": float((y == predicted).mean()), "macro_f1": float(np.mean(f1)),
            "log_loss": float(-np.log(np.clip(p[np.arange(len(y)), y], 1e-15, 1)).mean()),
            "brier_score": float(np.mean(np.sum((p - np.eye(3)[y]) ** 2, axis=1))),
            "confusion_matrix": confusion.tolist(), "class_f1": dict(zip(LABELS, map(float, f1)))}

def train(root, output, silver=None):
    output = Path(output)
    rows, fingerprint = write_dataset(root, output)
    if silver:
        from .dataset import reconcile_silver
        validation_spark = (SparkSession.builder.appName("MatchSilverReconciliation").master(os.getenv("SPARK_MASTER", "local[2]"))
                            .config("spark.driver.memory", "512m").config("spark.hadoop.fs.defaultFS", "file:///")
                            .config("spark.hadoop.dfs.blocksize", "67108864").config("spark.hadoop.parquet.block.size", "67108864").config("spark.hadoop.dfs.client.use.datanode.hostname", "true").config("spark.hadoop.dfs.replication", "2").getOrCreate())
        try:
            rows = reconcile_silver(validation_spark, rows, silver, output)
        finally:
            validation_spark.stop()
    train_rows = [r for r in rows if r["season"] == "2022-2023"]
    validation = sorted([r for r in rows if r["season"] == "2023-2024"], key=lambda r: (r["match_date"], r["match_id"]))
    tune_rows, calibrate_rows = validation[:190], validation[190:]
    test_rows = [r for r in rows if r["season"] == "2024-2025"]
    artifact = {"schema_version": 1, "labels": list(LABELS), "dataset_fingerprint": fingerprint,
                "model_version": "wdl_" + hashlib.sha256((fingerprint + "lr-v2.0").encode()).hexdigest()[:20],
                "numeric_metrics": [], "medians": {}, "formations": {}, "feature_names": [], "disabled_metrics": [], "removed_columns": [],
                "description": "Ước tính từ thống kê tổng kết trận, chưa phải dự đoán theo phút."}
    for key in METRICS:
        sides = {side: [r[side + "_stats"][key] for r in train_rows if r[side + "_stats"][key] is not None] for side in ("home", "away")}
        all_values = sides["home"] + sides["away"]
        if not sides["home"] or not sides["away"] or len(set(all_values)) <= 1:
            artifact["disabled_metrics"].append(key)
            artifact['removed_columns'].append({'column':key, 'reason':'all_missing_on_one_or_both_sides' if not sides['home'] or not sides['away'] else 'constant_training_value'})
            continue
        artifact["numeric_metrics"].append(key)
        for side in sides:
            artifact["medians"][side + "_" + key] = float(np.median(sides[side]))
        artifact["feature_names"].extend(["home_" + key, "away_" + key, "delta_" + key])
    for side in ("home", "away"):
        artifact["formations"][side] = sorted({r[side + "_stats"]["formation"] for r in train_rows if r[side + "_stats"]["formation"]})
        artifact["feature_names"].extend([side + "_formation_" + f for f in artifact["formations"][side]] + [side + "_formation_unknown"])
    size = len(artifact["feature_names"])
    artifact.update(means=[0.0] * size, scales=[1.0] * size)
    raw = np.stack([vector(r, artifact)[0] for r in train_rows])
    artifact["means"] = raw.mean(axis=0).tolist()
    std = raw.std(axis=0, ddof=1)
    artifact['feature_indices'] = np.flatnonzero(std > 1e-12).tolist()
    artifact['removed_columns'].extend({'column':n,'reason':'constant_after_training_only_imputation'} for i,n in enumerate(artifact['feature_names']) if std[i] <= 1e-12)
    artifact['feature_names'] = [artifact['feature_names'][i] for i in artifact['feature_indices']]
    artifact['means'] = raw.mean(axis=0)[artifact['feature_indices']].tolist()
    std = std[artifact['feature_indices']]
    artifact["scales"] = np.where(std > 1e-12, std, 1.0).tolist()
    xs = {name: np.stack([vector(r, artifact)[0] for r in selected]) for name, selected in
          [("train", train_rows), ("tune", tune_rows), ("calibration", calibrate_rows), ("test", test_rows)]}
    labels = {name: np.array([r["label"] for r in selected]) for name, selected in
              [("train", train_rows), ("tune", tune_rows), ("calibration", calibrate_rows), ("test", test_rows)]}
    spark = (SparkSession.builder.appName("MatchOutcomeTraining").master(os.getenv("SPARK_MASTER", "local[2]"))
             .config("spark.hadoop.fs.defaultFS", "file:///")
             .config("spark.driver.memory", "512m").config("spark.sql.shuffle.partitions", "4")
             .config("spark.hadoop.dfs.blocksize", "67108864").config("spark.hadoop.parquet.block.size", "67108864").config("spark.hadoop.dfs.client.use.datanode.hostname", "true").config("spark.hadoop.dfs.replication", "2")
             .config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    training = spark.createDataFrame([(float(y), Vectors.dense(x)) for y, x in zip(labels["train"], xs["train"])], ["label", "features"])
    candidates = []
    best, best_loss = None, float("inf")
    try:
        for reg in (0.01, 0.1, 1.0):
            model = LogisticRegression(family="multinomial", regParam=reg, elasticNetParam=0.0,
                                       maxIter=200, tol=1e-8, standardization=False).fit(training)
            coefficients, intercepts = model.coefficientMatrix.toArray(), model.interceptVector.toArray()
            p = softmax(xs["tune"] @ coefficients.T + intercepts)
            score = metrics(labels["tune"], p)
            candidates.append({"reg_param": reg, **score})
            print(f"LR reg={reg}: tuning log_loss={score['log_loss']:.6f}", flush=True)
            if score["log_loss"] < best_loss:
                best, best_loss = model, score["log_loss"]
                artifact.update(reg_param=reg, coefficients=coefficients.tolist(), intercepts=intercepts.tolist())
        calibration_logits = xs["calibration"] @ np.asarray(artifact["coefficients"]).T + artifact["intercepts"]
        temperatures = np.unique(np.concatenate([np.geomspace(0.05, 10, 401), [1.0]]))
        losses = [metrics(labels["calibration"], softmax(calibration_logits / t))["log_loss"] for t in temperatures]
        artifact["temperature"] = float(temperatures[int(np.argmin(losses))])
        parity_df = spark.createDataFrame([(i, Vectors.dense(vector(row, artifact)[0])) for i, row in enumerate(rows)], ["index", "features"])
        observed = best.transform(parity_df).select("index", "rawPrediction").collect()
        max_difference = 0.0
        for result in observed:
            expected = softmax(result["rawPrediction"].toArray() / artifact["temperature"])
            actual = np.array(list(predict(rows[result["index"]], artifact)["probabilities"].values()))
            max_difference = max(max_difference, float(np.max(np.abs(expected - actual))))
        if max_difference > 1e-8:
            raise RuntimeError(f"Spark/NumPy mismatch: {max_difference}")
        model_path = (output / "spark_model").resolve().as_uri()
        best.write().overwrite().save(model_path)
    finally:
        spark.stop()
    distribution = np.bincount(labels["train"], minlength=3) / len(train_rows)
    test_probabilities = softmax(xs["test"] @ np.asarray(artifact["coefficients"]).T / artifact["temperature"] + np.asarray(artifact["intercepts"]) / artifact["temperature"])
    report = {"labels": list(LABELS), "model_version": artifact["model_version"], "split_counts": {n: len(y) for n, y in labels.items()},
              "splits": {"train": "2022-2023", "tune": "first 190 matches of 2023-2024 ordered by date/id",
                         "calibration": "last 190 matches of 2023-2024", "test": "2024-2025 (untouched)"},
              "candidates": candidates, "reg_param": artifact["reg_param"], "temperature": artifact["temperature"],
              "test": metrics(labels["test"], test_probabilities), "baseline": metrics(labels["test"], np.tile(distribution, (len(test_rows), 1))),
              "spark_numpy_max_absolute_error": max_difference, "disabled_metrics": artifact["disabled_metrics"],
              "training_only_preprocessing": True, "all_source_parameters_preserved": True}
    report['removed_columns'] = artifact['removed_columns']
    catalog_path = output / 'feature_catalog.json'
    catalog = json.loads(catalog_path.read_text(encoding='utf-8'))
    for column in catalog:
        if column['feature'] in artifact['disabled_metrics']:
            column.update(role='excluded', reason='Hoàn toàn thiếu hoặc hằng số trong tập huấn luyện; xem removed_columns')
    catalog_path.write_text(json.dumps(catalog,ensure_ascii=False,indent=2),encoding='utf-8')
    for name, value in [("model.json", artifact), ("metrics.json", report)]:
        temporary = output / (name + ".tmp")
        temporary.write_text(json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False), encoding="utf-8")
        temporary.replace(output / name)
        version_dir = output / "versions" / artifact["model_version"]
        version_dir.mkdir(parents=True, exist_ok=True)
        (version_dir / name).write_text(json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False), encoding="utf-8")
    with (output / "predictions.csv").open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=["match_id", "season", "actual", *LABELS, "model_version"])
        writer.writeheader()
        for row in rows:
            writer.writerow({"match_id": row["match_id"], "season": row["season"], "actual": LABELS[row["label"]],
                             **predict(row, artifact)["probabilities"], "model_version": artifact["model_version"]})
    (output / "match_outcome.html").write_text("<!doctype html><html lang='vi'><meta charset='utf-8'><title>Dự đoán kết quả trận</title>"
        "<style>body{max-width:1000px;margin:50px auto;font:16px system-ui;background:#edf3f8;color:#172943}pre{background:white;padding:24px;overflow:auto}</style>"
        "<h1>Ước tính thắng · hòa · thua</h1><p>Model dùng thống kê tổng kết trận. Bộ kiểm thử: mùa 2024–2025.</p><pre>"
        + json.dumps(report, ensure_ascii=False, indent=2) + "</pre></html>", encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False), flush=True)
    return report

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=str(Path(__file__).resolve().parents[4]))
    parser.add_argument("--output", default=str(Path(__file__).resolve().parent / "outputs"))
    parser.add_argument("--silver")
    args = parser.parse_args()
    train(args.root, args.output, args.silver)
