#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Automatic AI Metrics Logger

This module appends metrics for each model performance evaluation
to state/ai_metrics.jsonl in a canonical, searchable format.
"""

import json
import time
from pathlib import Path
from typing import Dict, Optional

from sklearn.metrics import accuracy_score, roc_auc_score  # Optional for AUC

AI_METRICS_FILE = Path("state/ai_metrics.jsonl")


def _now_ms() -> int:
    return int(time.time() * 1000)


def _append_jsonl(path: Path, obj: Dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("ab") as f:
        f.write(json.dumps(obj).encode("utf-8") + b"\n")


def compute_basic_metrics(
    y_true,
    y_pred,
    y_score: Optional[list] = None,
) -> Dict[str, float]:
    """
    Compute core performance metrics.
    Requires sklearn for AUC.
    """
    metrics: Dict[str, float] = {}

    try:
        metrics["accuracy"] = float(accuracy_score(y_true, y_pred))
    except Exception:
        metrics["accuracy"] = None

    try:
        if y_score is not None:
            # roc_auc_score may fail if only one class present
            metrics["auc"] = float(roc_auc_score(y_true, y_score))
        else:
            metrics["auc"] = None
    except Exception:
        metrics["auc"] = None

    return metrics


def log_model_metrics(
    model_id: str,
    model_version: str,
    strategy_id: str,
    sub_uid: str,
    y_true,
    y_pred,
    y_score: Optional[list] = None,
    extra: Optional[Dict] = None,
) -> None:
    """
    Append a metric record for AI performance.

    Args:
        model_id: logical model name
        model_version: version string
        strategy_id: strategy label/order
        sub_uid: subaccount identifier
        y_true: ground truth labels
        y_pred: predicted labels
        y_score: predicted probabilities (for AUC)
        extra: any custom metrics (drawdown, expectancy, etc.)
    """
    if extra is None:
        extra = {}

    # compute basic metrics
    base_metrics = compute_basic_metrics(y_true, y_pred, y_score)

    # build full record
    record = {
        "ts_ms": _now_ms(),
        "model_id": model_id,
        "model_version": model_version,
        "strategy_id": strategy_id,
        "sub_uid": sub_uid,
        "metrics": {
            **base_metrics,
            **(extra or {}),
        },
    }

    # append to metrics file
    _append_jsonl(AI_METRICS_FILE, record)
