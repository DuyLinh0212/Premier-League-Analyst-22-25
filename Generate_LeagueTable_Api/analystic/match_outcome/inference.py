"""Portable NumPy scorer for exported LR, KNN and Random Forest models."""
import json
import re
from pathlib import Path
import numpy as np
from .schema import LABELS, validate_stats

def load_published_artifact(directory):
    directory = Path(directory)
    pointer = directory / 'published.json'
    if pointer.exists():
        version = json.loads(pointer.read_text(encoding='utf-8'))['model_version']
        if not re.fullmatch(r'wdl_[0-9a-f]{20}',version):
            raise ValueError('Invalid published model version')
        return load_artifact(directory / 'versions' / version)
    return load_artifact(directory)

def vector(record, artifact):
    sides = {side: validate_stats(record[side + "_stats"]) for side in ("home", "away")}
    values, missing = [], []
    for key in artifact["numeric_metrics"]:
        h, a = sides["home"][key], sides["away"][key]
        for side, value in (("home", h), ("away", a)):
            if value is None:
                missing.append(side + "." + key)
        h = artifact["medians"]["home_" + key] if h is None else h
        a = artifact["medians"]["away_" + key] if a is None else a
        values.extend((h, a, h - a))
    for side in ("home", "away"):
        formation = sides[side]["formation"]
        categories = artifact["formations"][side]
        values.extend(1.0 if formation == category else 0.0 for category in categories)
        values.append(1.0 if formation not in categories else 0.0)
        if formation is None:
            missing.append(side + ".formation")
    raw = np.asarray(values, dtype=float)
    if 'feature_indices' in artifact:
        raw = raw[artifact['feature_indices']]
    if len(raw) != len(artifact["feature_names"]):
        raise ValueError("Model feature order mismatch")
    return (raw - np.asarray(artifact["means"])) / np.asarray(artifact["scales"]), missing

def softmax(logits):
    logits = np.asarray(logits, dtype=float)
    exps = np.exp(logits - logits.max(axis=-1, keepdims=True))
    return exps / exps.sum(axis=-1, keepdims=True)

def predict(record, artifact):
    x, missing = vector(record, artifact)
    logits = base_logits(x, artifact)
    probabilities = softmax(logits / artifact.get("temperature", 1.0))
    return {"probabilities": dict(zip(LABELS, map(float, probabilities))),
            "model_version": artifact["model_version"], "imputed_fields": missing,
            "estimated_outcome": LABELS[int(probabilities.argmax())]}

def base_logits(x, artifact):
    kind = artifact.get('algorithm', 'logistic_regression')
    if kind == 'logistic_regression':
        return np.asarray(artifact['coefficients']) @ x + np.asarray(artifact['intercepts'])
    if kind == 'knn_classifier':
        references = np.asarray(artifact['reference_vectors'])
        distances = np.sum((references - x) ** 2, axis=1)
        neighbors = np.argsort(distances, kind='stable')[:artifact['k']]
        counts = np.bincount(np.asarray(artifact['reference_labels'])[neighbors], minlength=3)
        probabilities = (counts + artifact['smoothing'] * np.asarray(artifact['class_prior'])) / (len(neighbors) + artifact['smoothing'])
    elif kind == 'random_forest':
        probabilities = np.zeros(3)
        for tree in artifact['trees']:
            node = tree
            while 'probabilities' not in node:
                node = node['left'] if x[node['feature_index']] <= node['threshold'] else node['right']
            probabilities += node['probabilities']
        probabilities /= len(artifact['trees'])
    else:
        raise ValueError('Unsupported model algorithm: ' + kind)
    return np.log(np.clip(probabilities, 1e-15, 1))

def load_artifact(directory):
    artifact = json.loads((Path(directory) / "model.json").read_text(encoding="utf-8"))
    if artifact["labels"] != list(LABELS):
        raise ValueError("Invalid model labels")
    return artifact
