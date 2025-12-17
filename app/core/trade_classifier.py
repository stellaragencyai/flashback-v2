#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Trade Classifier v2.2

Purpose
-------
Unified classifier used by the executor + setup memory.

Two modes:

1) NEW AI GATE MODE (used by executor_v2):
    classify(signal: dict, strat_id: str) -> dict

    Returns:
      {
        "allow": bool,         # model/policy-based yes/no
        "score": float | None, # probability from model [0,1] if available
        "reason": str,         # human-readable summary
        "features": dict       # feature dict used for scoring & logging
      }

    - Uses trained model from models/setup_classifier.pkl if present.
    - Uses per-strategy min_ai_score from setup_memory_policy if available.
    - Falls back gracefully if model/meta/policy missing or broken.

2) LEGACY LABEL MODE (backward compat):
    classify(signal: dict, features: dict) -> str

    - Deterministic heuristic label:
        "breakout_trend", "pullback_trend", "range_fade",
        "vol_squeeze_break", "news_spike", "trend_momentum", "unknown"
    - The old behavior is preserved via an internal helper.

Executor usage (new):
    from app.core.trade_classifier import classify as classify_trade
    clf = classify_trade(signal, strat_id)

Other legacy usage (if any) still works:
    label = classify(signal, features_dict)
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

try:
    from app.core.logger import get_logger
except Exception:
    import logging

    def get_logger(name: str) -> "logging.Logger":  # type: ignore
        """
        Minimal fallback logger used when app.core.logger is unavailable.
        """
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


log = get_logger("trade_classifier")

# ROOT / models path
try:
    from app.core.config import settings
except Exception:
    class _DummySettings:  # type: ignore
        ROOT: Path = Path(__file__).resolve().parents[2]
    settings = _DummySettings()  # type: ignore

ROOT: Path = settings.ROOT
MODELS_DIR: Path = ROOT / "models"
MODEL_PATH: Path = MODELS_DIR / "setup_classifier.pkl"
META_PATH: Path = MODELS_DIR / "setup_classifier_meta.json"

# Optional policy integration (per-strategy min_ai_score)
try:
    from app.ai.setup_memory_policy import get_min_ai_score as _policy_get_min_ai_score  # type: ignore

    def _policy_min_ai_score(strategy_id: str) -> float:
        try:
            v = float(_policy_get_min_ai_score(strategy_id))
        except Exception:
            v = 0.5
        if v < 0.0:
            return 0.0
        if v > 1.0:
            return 1.0
        return v
except Exception:
    # Fallback: fixed 0.50 threshold if policy module not available
    def _policy_min_ai_score(strategy_id: str) -> float:  # type: ignore[override]
        return 0.5


# ---------------------------------------------------------------------------
# Legacy heuristic labeler (kept as-is)
# ---------------------------------------------------------------------------

def _get_lower(d: Dict[str, Any], key: str) -> str:
    v = d.get(key)
    if v is None:
        return ""
    return str(v).strip().lower()


def _legacy_label(signal: Dict[str, Any], features: Dict[str, Any]) -> str:
    """
    Legacy deterministic tagger:
        (signal, features) -> short label string
    """

    reason = _get_lower(signal, "reason")
    pattern = _get_lower(signal, "pattern")
    regime = _get_lower(features, "regime")
    structure = _get_lower(features, "structure") or _get_lower(features, "market_structure")

    try:
        adx = float(features.get("adx", 0.0))
    except Exception:
        adx = 0.0
    try:
        atr_pct = float(features.get("atr_pct", 0.0))
    except Exception:
        atr_pct = 0.0
    try:
        vol_z = float(features.get("vol_z", 0.0))
    except Exception:
        vol_z = 0.0

    # 1) Strong trend + breakout-ish reason
    if adx >= 20 and ("breakout" in reason or "breakout" in pattern):
        if "pullback" in reason or "retest" in reason:
            return "pullback_trend"
        return "breakout_trend"

    # 2) Clear range structure
    if "range" in structure or "range" in reason:
        if any(k in reason for k in ("fade", "revert", "mean")):
            return "range_fade"
        return "range_play"

    # 3) Volatility squeeze then pop
    if "squeeze" in reason or "squeeze" in pattern:
        if vol_z > 1.5 or atr_pct > 1.0:
            return "vol_squeeze_break"
        return "vol_squeeze"

    # 4) News-ish spikes
    if any(k in reason for k in ("news", "event", "fomc", "earnings")):
        return "news_spike"

    # 5) Default momentum in trend
    if adx >= 20:
        return "trend_momentum"

    return "unknown"


# ---------------------------------------------------------------------------
# Model loader / live feature extraction
# ---------------------------------------------------------------------------

_MODEL = None
_MODEL_FEATURES: Optional[list[str]] = None
_MODEL_META: Dict[str, Any] = {}


def _load_model_once() -> None:
    """
    Lazy-load the classifier model and its metadata.

    On any failure, we drop into "no_model_loaded" mode and keep
    allowing trades by default while still returning features.
    """
    global _MODEL, _MODEL_FEATURES, _MODEL_META

    if _MODEL is not None:
        return

    if not MODEL_PATH.exists() or not META_PATH.exists():
        log.info("No trained model artifacts found; classifier will run in fallback mode.")
        _MODEL = None
        _MODEL_FEATURES = None
        _MODEL_META = {}
        return

    try:
        import joblib  # type: ignore
    except Exception as e:
        log.warning("joblib not available to load model: %r", e)
        _MODEL = None
        _MODEL_FEATURES = None
        _MODEL_META = {}
        return

    try:
        _MODEL = joblib.load(MODEL_PATH)
        meta_raw = META_PATH.read_text()
        _MODEL_META = json.loads(meta_raw)
        feat_names = _MODEL_META.get("feature_names") or []
        _MODEL_FEATURES = list(feat_names) if isinstance(feat_names, (list, tuple)) else []
        log.info("Loaded setup classifier model from %s", MODEL_PATH)
    except Exception as e:
        log.exception("Failed to load model/meta: %r", e)
        _MODEL = None
        _MODEL_FEATURES = None
        _MODEL_META = {}


def _session_to_int(session: str) -> int:
    s = (session or "").upper()
    if s == "ASIA":
        return 0
    if s == "LONDON":
        return 1
    if s == "NEW_YORK":
        return 2
    if s == "POST":
        return 3
    return -1


def _derive_session_from_ts(ts_ms: Optional[int]) -> str:
    """
    Rough session from timestamp (UTC). If ts is missing, use current time.
    """
    import datetime as dt

    if ts_ms is None:
        now = dt.datetime.utcnow()
    else:
        now = dt.datetime.utcfromtimestamp(ts_ms / 1000.0)

    hour = now.hour
    if 0 <= hour < 7:
        return "ASIA"
    if 7 <= hour < 13:
        return "LONDON"
    if 13 <= hour < 20:
        return "NEW_YORK"
    return "POST"


def _extract_live_features(signal: Dict[str, Any]) -> Tuple[Dict[str, Any], list[float]]:
    """
    Build a feature dict + vector for the live signal, aligned (as much as possible)
    with the features used in training.

    Training feature names (from train_models.py):
        [
          "side_sign",
          "atr_like",
          "atr_pct",
          "range_mean",
          "range_std",
          "volume_zscore",
          "trend_dir",
          "trend_strength",
          "entry_hour",
          "entry_dow",
          "session_int"
        ]
    """
    import datetime as dt

    side_raw = str(signal.get("side") or "").lower()
    if side_raw in ("buy", "long"):
        side_sign = 1
    elif side_raw in ("sell", "short"):
        side_sign = -1
    else:
        side_sign = 0

    # Numeric fields with graceful fallback
    def _f(keys, default=0.0) -> float:
        for k in keys:
            if k in signal and signal[k] is not None:
                try:
                    return float(signal[k])
                except Exception:
                    continue
        return float(default)

    atr_like = _f(["atr_like"], 0.0)
    atr_pct = _f(["atr_pct", "atr_percent"], 0.0)
    range_mean = _f(["range_mean"], 0.0)
    range_std = _f(["range_std"], 0.0)

    # Volume zscore might be named in different ways
    volume_zscore = _f(["volume_zscore", "vol_z", "volume_z"], 0.0)

    # Trend dir/strength could be precomputed; if not, we leave neutral
    trend_dir = _f(["trend_dir"], 0.0)
    trend_strength = _f(["trend_strength"], 0.0)

    ts = signal.get("ts") or signal.get("timestamp")
    try:
        ts_ms = int(ts)
    except Exception:
        ts_ms = None

    if ts_ms is not None:
        dt_obj = dt.datetime.utcfromtimestamp(ts_ms / 1000.0)
    else:
        dt_obj = dt.datetime.utcnow()

    entry_hour = dt_obj.hour
    entry_dow = dt_obj.weekday()

    session = _derive_session_from_ts(ts_ms)
    session_int = _session_to_int(session)

    feature_dict: Dict[str, Any] = {
        "side_sign": side_sign,
        "atr_like": atr_like,
        "atr_pct": atr_pct,
        "range_mean": range_mean,
        "range_std": range_std,
        "volume_zscore": volume_zscore,
        "trend_dir": trend_dir,
        "trend_strength": trend_strength,
        "entry_hour": entry_hour,
        "entry_dow": entry_dow,
        "session": session,
        "session_int": session_int,
    }

    # Default feature order used by training
    default_names = [
        "side_sign",
        "atr_like",
        "atr_pct",
        "range_mean",
        "range_std",
        "volume_zscore",
        "trend_dir",
        "trend_strength",
        "entry_hour",
        "entry_dow",
        "session_int",
    ]

    names = _MODEL_FEATURES if _MODEL_FEATURES else default_names
    vec = [float(feature_dict.get(name, 0.0)) for name in names]

    return feature_dict, vec

# ──────────────────────────────────────────────────────────────────────────
# Regime-Aware Model Loader & Classifier Enhancements
# ──────────────────────────────────────────────────────────────────────────

# Internal storage for regime models
_REGIME_MODELS: Dict[str, Any] = {}
_REGIME_FEATURES: Dict[str, list[str]] = {}
_MODEL_LOADED = False

# Default global model (fallback)
_GLOBAL_MODEL = None
_GLOBAL_FEATURES: list[str] = []

def _load_models_once() -> None:
    """
    Lazy load all regime expert models found in models/ directory.
    Expected naming:
      - setup_classifier_{regime}.pkl
      - setup_classifier_{regime}_meta.json
    """
    global _REGIME_MODELS, _REGIME_FEATURES, _GLOBAL_MODEL, _GLOBAL_FEATURES, _MODEL_LOADED

    if _MODEL_LOADED:
        return
    _MODEL_LOADED = True

    import joblib
    from pathlib import Path

    models_root = Path(ROOT) / "models"
    if not models_root.exists():
        log.info("No models directory found; classifier will operate with no models.")
        return

    # Load all pickles ending with _classifier_*.pkl
    for p in models_root.glob("setup_classifier_*.pkl"):
        try:
            regimen = p.stem.replace("setup_classifier_", "")
            model_obj = joblib.load(p)
            _REGIME_MODELS[regimen] = model_obj

            # Try corresponding meta file
            meta_path = models_root / f"{p.stem}_meta.json"
            if meta_path.exists():
                try:
                    raw_meta = meta_path.read_text()
                    meta = json.loads(raw_meta)
                    feat_names = meta.get("feature_names") or []
                    _REGIME_FEATURES[regimen] = list(feat_names)
                except Exception:
                    _REGIME_FEATURES[regimen] = []
            else:
                _REGIME_FEATURES[regimen] = []

            log.info(f"Loaded regime model '{regimen}'")
        except Exception as e:
            log.warning(f"Failed to load regime model from {p}: {e}")

    # Optionally also load a global fallback model
    global_path = models_root / "setup_classifier.pkl"
    global_meta = models_root / "setup_classifier_meta.json"
    if global_path.exists():
        try:
            _GLOBAL_MODEL = joblib.load(global_path)
            log.info("Loaded global fallback classifier")
            if global_meta.exists():
                raw_meta = global_meta.read_text()
                gm = json.loads(raw_meta)
                _GLOBAL_FEATURES = gm.get("feature_names") or []
        except Exception as e:
            log.warning("Failed to load global fallback classifier: %r", e)

def _pick_model_for_regime(regime: str):
    """
    Return (model, feature_names, regime_key_used)
    """
    # Exact regime match
    if regime in _REGIME_MODELS:
        return _REGIME_MODELS[regime], _REGIME_FEATURES.get(regime, []), regime
    # Fallback: try lowercase keys
    low = regime.lower()
    for rkey in _REGIME_MODELS:
        if rkey.lower() == low:
            return _REGIME_MODELS[rkey], _REGIME_FEATURES.get(rkey, []), rkey
    # Fallback: global if available
    if _GLOBAL_MODEL is not None:
        return _GLOBAL_MODEL, _GLOBAL_FEATURES, "global"
    # No model at all
    return None, [], None

def _classify_ai(signal: Dict[str, Any], strat_id: str) -> Dict[str, Any]:
    """
    Regime-aware AI classification:
    """
    _load_models_once()

    # Build features for this signal
    features_dict, vec = _extract_live_features(signal)

    # Determine regime (from normalized feature store)
    # Expect it to be present in `signal["regime"]` or features_dict from builder
    regime = str(signal.get("regime") or features_dict.get("regime") or "other")

    model_obj, feature_names, used_regime = _pick_model_for_regime(regime)

    # Attach regime tag
    features_dict["regime"] = regime
    features_dict["used_regime_model"] = used_regime

    # Check absence of model
    if model_obj is None:
        return {
            "allow": True,
            "score": None,
            "reason": f"no_model_for_regime_{regime}",
            "features": features_dict
        }

    # Attempt model inference
    try:
        # If needed, reorder vec to match regime model features
        # We assume _extract_live_features makes vec in a default order.
        probs = model_obj.predict_proba([vec])[0]
        score = float(probs[1]) if len(probs) > 1 else float(probs[0])
    except Exception as e:
        log.warning(f"Model inference failed for regime={regime}: {e}")
        return {
            "allow": True,
            "score": None,
            "reason": f"inference_error:{e}",
            "features": features_dict
        }

    # Apply per-strategy policy
    try:
        min_score = _policy_min_ai_score(strat_id)
    except Exception:
        min_score = 0.5
    features_dict["min_ai_score"] = float(min_score)

    allow = bool(score >= min_score)
    reason = "score_ok" if allow else f"below_min_ai_score_{min_score:.3f}"

    return {
        "allow": allow,
        "score": score,
        "reason": reason,
        "features": features_dict,
    }

# ---------------------------------------------------------------------------
# Public entrypoint with dual behavior
# ---------------------------------------------------------------------------

def classify(*args, **kwargs):
    """
    Public entry with dual behavior:

    1) NEW AI MODE (executor_v2):
        classify(signal: dict, strat_id: str) -> dict

    2) LEGACY LABEL MODE:
        classify(signal: dict, features: dict) -> str

    We branch based on types of the second arg.
    """
    # Keyword-based dispatch (just in case)
    if "strat_id" in kwargs or "strategy_id" in kwargs:
        signal = kwargs.get("signal") or (args[0] if args else {})
        strat_id = kwargs.get("strat_id") or kwargs.get("strategy_id")
        return _classify_ai(signal, str(strat_id))

    # Positional dispatch
    if len(args) == 2:
        a0, a1 = args
        # New AI usage: (signal, strat_id) where strat_id is a string
        if isinstance(a1, str):
            return _classify_ai(a0, a1)
        # Legacy usage: (signal, features_dict)
        if isinstance(a1, dict):
            return _legacy_label(a0, a1)

    # Fallback: try to guess
    if len(args) == 1 and isinstance(args[0], dict):
        # Just signal? not great; treat as "unknown"
        return "unknown"

    raise TypeError(
        "classify() expected (signal, strat_id:str) or (signal, features:dict); "
        f"got args={args}, kwargs={kwargs}"
    )
