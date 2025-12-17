#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Train one classifier per regime cluster.

Produces:
  models/setup_classifier_{regime}.pkl
  models/setup_classifier_{regime}_meta.json

Regimes are inferred from each feature row’s "regime" field.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Tuple, DefaultDict
from collections import defaultdict

import orjson

try:
    from app.core.logger import get_logger
except Exception:
    import logging

    def get_logger(name: str) -> "logging.Logger":
        logger_ = logging.getLogger(name)
        if not logger_.handlers:
            handler = logging.StreamHandler()
            fmt = logging.Formatter(
                "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
            )
            handler.setFormatter(fmt)
        logger_.addHandler(handler)
        logger_.setLevel(logging.INFO)
        return logger_

log = get_logger("train_regime_models")

try:
    from app.core.config import settings
except Exception:
    class _DummySettings:
        from pathlib import Path
        ROOT = Path(__file__).resolve().parents[2]
    settings = _DummySettings()

ROOT: Path = settings.ROOT
STATE_DIR: Path = ROOT / "state"
MODELS_DIR: Path = ROOT / "models"

STATE_DIR.mkdir(parents=True, exist_ok=True)
MODELS_DIR.mkdir(parents=True, exist_ok=True)

FEATURE_STORE_PATH: Path = STATE_DIR / "feature_store.jsonl"


# ---------------------------------------------------------------------------
# Load dataset
# ---------------------------------------------------------------------------

def _load_feature_rows() -> List[Dict[str, Any]]:
    if not FEATURE_STORE_PATH.exists():
        log.error("feature_store.jsonl not found at %s", FEATURE_STORE_PATH)
        return []
    rows: List[Dict[str, Any]] = []
    with FEATURE_STORE_PATH.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                rows.append(orjson.loads(line))
            except Exception:
                pass
    log.info("Loaded %d feature rows.", len(rows))
    return rows


# ---------------------------------------------------------------------------
# Partition data by regime
# ---------------------------------------------------------------------------

def partition_by_regime(rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    per: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        regime = str(r.get("regime") or "unknown").lower()
        per[regime].append(r)
    return per


# ---------------------------------------------------------------------------
# Build X,y similar to original train_models
# ---------------------------------------------------------------------------

def build_dataset(rows: List[Dict[str, Any]]) -> Tuple[List[List[float]], List[int], Dict[str, Any]]:
    X, y = [], []
    feature_names = [
        "side_sign","atr_like","atr_pct","range_mean","range_std",
        "volume_zscore","trend_dir","trend_strength","entry_hour","entry_dow",
    ]
    extra_feature_names = ["session_int"]

    total, discarded = 0, 0

    for row in rows:
        total += 1
        try:
            pnl_r = float(row.get("pnl_r", 0.0))
        except Exception:
            pnl_r = 0.0
        label = 1 if pnl_r > 0 else 0

        try:
            fv = [float(row.get(name) or 0.0) for name in feature_names]
            session_int = float(row.get("session_int") or 0.0)
            fv.append(session_int)
        except Exception:
            discarded += 1
            continue

        X.append(fv)
        y.append(label)

    info = {
        "total_rows": total,
        "used_rows": len(X),
        "discarded_rows": discarded,
        "feature_names": feature_names + extra_feature_names,
    }
    return X, y, info


# ---------------------------------------------------------------------------
# Train single model
# ---------------------------------------------------------------------------

def train_model(X: List[List[float]], y: List[int]):
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import accuracy_score, roc_auc_score
    from sklearn.model_selection import train_test_split
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y if len(set(y)) > 1 else None
    )

    pipeline = Pipeline(steps=[
        ("scaler", StandardScaler()),
        ("clf", LogisticRegression(max_iter=200)),
    ])
    pipeline.fit(X_train, y_train)

    y_pred = pipeline.predict(X_test) if X_test else []
    y_prob = (
        pipeline.predict_proba(X_test)[:, 1].tolist()
        if X_test and hasattr(pipeline, "predict_proba")
        else []
    )

    metrics = {
        "accuracy": None, "roc_auc": None
    }
    try:
        from sklearn.metrics import accuracy_score, roc_auc_score
        if y_test:
            metrics["accuracy"] = float(accuracy_score(y_test, y_pred))
            try:
                metrics["roc_auc"] = float(roc_auc_score(y_test, y_prob))
            except Exception:
                pass
    except Exception:
        pass

    return {"model": pipeline, "metrics": metrics}


# ---------------------------------------------------------------------------
# Persist
# ---------------------------------------------------------------------------

def _save_artifacts(model_obj: Any, meta: Dict[str, Any], regime: str) -> None:
    import joblib
    pkl = MODELS_DIR / f"setup_classifier_{regime}.pkl"
    meta_path = MODELS_DIR / f"setup_classifier_{regime}_meta.json"

    joblib.dump(model_obj, pkl)
    log.info("Saved regime model at %s", pkl)

    meta_path.write_text(json.dumps(meta, indent=2))
    log.info("Saved meta at %s", meta_path)


# ---------------------------------------------------------------------------
# Main script
# ---------------------------------------------------------------------------

def main() -> None:
    rows = _load_feature_rows()
    if not rows:
        log.error("No data to train on.")
        return

    by_regime = partition_by_regime(rows)
    log.info("Found regimes: %s", list(by_regime.keys()))

    for regime, rrows in by_regime.items():
        X, y, info = build_dataset(rrows)
        log.info("Regime=%s: rows=%s used=%s", regime, info["total_rows"], info["used_rows"])
        if info["used_rows"] < 10:
            log.warning("Too few rows for regime %s; skipping.", regime)
            continue

        try:
            result = train_model(X, y)
        except Exception as e:
            log.exception("Train fail for regime %s: %r", regime, e)
            continue

        model = result["model"]
        meta = {
            "regime": regime,
            "feature_names": info["feature_names"],
            "dataset_info": info,
            "metrics": result["metrics"],
        }
        _save_artifacts(model, meta, regime)

if __name__ == "__main__":
    main()
