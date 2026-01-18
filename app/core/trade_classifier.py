#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Trade Classifier v2.3 (Regime-Aware + Score Fallback + Feature Alignment)

Purpose
-------
Unified classifier used by executor_v2 + setup memory.

NEW AI GATE MODE (executor_v2):
    classify(signal: dict, strat_id: str) -> dict
Returns:
  {
    "allow": bool,
    "score": float | None,
    "reason": str,
    "features": dict
  }

LEGACY LABEL MODE (backward compat):
    classify(signal: dict, features: dict) -> str
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, List


# ──────────────────────────────────────────────────────────────────────────
# Logger
# ──────────────────────────────────────────────────────────────────────────
try:
    from app.core.logger import get_logger
except Exception:
    import logging

    def get_logger(name: str) -> "logging.Logger":  # type: ignore
        logger_ = logging.getLogger(name)
        if not logger_.handlers:
            handler = logging.StreamHandler()
            fmt = logging.Formatter("%(asctime)s [%(levelname)s] [%(name)s] %(message)s")
            handler.setFormatter(fmt)
            logger_.addHandler(handler)
        logger_.setLevel(logging.INFO)
        return logger_


log = get_logger("trade_classifier")


# ──────────────────────────────────────────────────────────────────────────
# ROOT / models path
# ──────────────────────────────────────────────────────────────────────────
try:
    from app.core.config import settings
except Exception:
    class _DummySettings:  # type: ignore
        ROOT: Path = Path(__file__).resolve().parents[2]
    settings = _DummySettings()  # type: ignore

ROOT: Path = settings.ROOT
MODELS_DIR: Path = ROOT / "models"


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
    def _policy_min_ai_score(strategy_id: str) -> float:  # type: ignore[override]
        return 0.5


# ──────────────────────────────────────────────────────────────────────────
# Legacy heuristic labeler (kept as-is)
# ──────────────────────────────────────────────────────────────────────────
def _get_lower(d: Dict[str, Any], key: str) -> str:
    v = d.get(key)
    if v is None:
        return ""
    return str(v).strip().lower()


def _legacy_label(signal: Dict[str, Any], features: Dict[str, Any]) -> str:
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

    if adx >= 20 and ("breakout" in reason or "breakout" in pattern):
        if "pullback" in reason or "retest" in reason:
            return "pullback_trend"
        return "breakout_trend"

    if "range" in structure or "range" in reason:
        if any(k in reason for k in ("fade", "revert", "mean")):
            return "range_fade"
        return "range_play"

    if "squeeze" in reason or "squeeze" in pattern:
        if vol_z > 1.5 or atr_pct > 1.0:
            return "vol_squeeze_break"
        return "vol_squeeze"

    if any(k in reason for k in ("news", "event", "fomc", "earnings")):
        return "news_spike"

    if adx >= 20:
        return "trend_momentum"

    return "unknown"


# ──────────────────────────────────────────────────────────────────────────
# Session helpers
# ──────────────────────────────────────────────────────────────────────────
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


# ──────────────────────────────────────────────────────────────────────────
# Feature extraction (reads from signal["features"] first)
# ──────────────────────────────────────────────────────────────────────────
DEFAULT_FEATURE_ORDER: List[str] = [
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


def _pick_feature_source(signal: Dict[str, Any]) -> Dict[str, Any]:
    """
    Prefer computed feature payloads if present.
    Supported:
      - signal["features"] (dict)
      - signal["setup_context"]["features"] (dict)
    Fallback:
      - signal itself
    """
    f = signal.get("features")
    if isinstance(f, dict):
        return f
    sc = signal.get("setup_context")
    if isinstance(sc, dict):
        f2 = sc.get("features")
        if isinstance(f2, dict):
            return f2
    return signal


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return float(default)
        return float(v)
    except Exception:
        return float(default)


def _extract_live_features(signal: Dict[str, Any]) -> Dict[str, Any]:
    import datetime as dt

    side_raw = str(signal.get("side") or "").lower()
    if side_raw in ("buy", "long"):
        side_sign = 1
    elif side_raw in ("sell", "short"):
        side_sign = -1
    else:
        side_sign = 0

    src = _pick_feature_source(signal)

    atr_like = _safe_float(src.get("atr_like"), 0.0)
    atr_pct = _safe_float(src.get("atr_pct", src.get("atr_percent")), 0.0)

    range_mean = _safe_float(src.get("range_mean"), 0.0)
    range_std = _safe_float(src.get("range_std"), 0.0)

    volume_zscore = _safe_float(
        src.get("volume_zscore", src.get("vol_z", src.get("volume_z"))), 0.0
    )

    trend_dir = _safe_float(src.get("trend_dir"), 0.0)
    trend_strength = _safe_float(src.get("trend_strength"), 0.0)

    # Timestamp: try common keys (ts_ms is best)
    ts_any = signal.get("ts_ms", signal.get("ts", signal.get("timestamp")))
    ts_ms: Optional[int]
    try:
        ts_ms = int(ts_any) if ts_any is not None else None
    except Exception:
        ts_ms = None

    if ts_ms is not None:
        dt_obj = dt.datetime.utcfromtimestamp(ts_ms / 1000.0)
    else:
        dt_obj = dt.datetime.utcnow()

    entry_hour = int(dt_obj.hour)
    entry_dow = int(dt_obj.weekday())

    session = str(src.get("session") or _derive_session_from_ts(ts_ms))
    session_int = _session_to_int(session)

    regime = str(src.get("regime", signal.get("regime", "other")))

    return {
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
        "regime": regime,
    }


def _vec_from_features(features: Dict[str, Any], names: List[str]) -> List[float]:
    return [_safe_float(features.get(n), 0.0) for n in names]


def _fallback_score(features: Dict[str, Any]) -> float:
    """
    Deterministic fallback score in [0,1] when no ML model artifacts exist.

    This keeps ai_policy_log useful (score is never null) and lets you test gating
    before training.
    """
    # Base
    s = 0.50

    # Trend strength helps
    ts = _safe_float(features.get("trend_strength"), 0.0)
    s += max(-0.15, min(0.15, ts * 0.05))

    # Volume zscore helps (cap contribution)
    vz = _safe_float(features.get("volume_zscore"), 0.0)
    s += max(-0.15, min(0.15, vz * 0.05))

    # ATR% mild penalty if too high (sloppy conditions), mild boost if moderate
    ap = _safe_float(features.get("atr_pct"), 0.0)
    if ap >= 2.0:
        s -= 0.05
    elif 0.2 <= ap <= 1.2:
        s += 0.03

    # Session bias (tiny)
    sess = str(features.get("session") or "").upper()
    if sess == "NEW_YORK":
        s += 0.02
    elif sess == "POST":
        s -= 0.02

    # Clamp
    if s < 0.0:
        return 0.0
    if s > 1.0:
        return 1.0
    return float(s)


# ──────────────────────────────────────────────────────────────────────────
# Regime-Aware Model Loader
# ──────────────────────────────────────────────────────────────────────────
_REGIME_MODELS: Dict[str, Any] = {}
_REGIME_FEATURES: Dict[str, List[str]] = {}
_GLOBAL_MODEL: Any = None
_GLOBAL_FEATURES: List[str] = []
_MODELS_LOADED: bool = False

_LAST_DEBUG_TS: float = 0.0


def _load_models_once() -> None:
    global _MODELS_LOADED, _GLOBAL_MODEL, _GLOBAL_FEATURES

    if _MODELS_LOADED:
        return
    _MODELS_LOADED = True

    if not MODELS_DIR.exists():
        log.info("No models directory found at %s; classifier will operate with fallback scoring.", MODELS_DIR)
        return

    try:
        import joblib  # type: ignore
    except Exception as e:
        log.warning("joblib not available; classifier will operate with fallback scoring. err=%r", e)
        return

    # Load regime models: setup_classifier_{regime}.pkl
    for p in MODELS_DIR.glob("setup_classifier_*.pkl"):
        # Avoid double-loading the global model if it exists as setup_classifier.pkl
        if p.name == "setup_classifier.pkl":
            continue

        try:
            regimen = p.stem.replace("setup_classifier_", "")
            model_obj = joblib.load(p)
            _REGIME_MODELS[regimen] = model_obj

            meta_path = MODELS_DIR / f"{p.stem}_meta.json"
            feat_names: List[str] = []
            if meta_path.exists():
                try:
                    meta = json.loads(meta_path.read_text(encoding="utf-8"))
                    raw = meta.get("feature_names") or []
                    if isinstance(raw, (list, tuple)):
                        feat_names = [str(x) for x in raw]
                except Exception:
                    feat_names = []
            _REGIME_FEATURES[regimen] = feat_names
            log.info("Loaded regime model '%s' (%s)", regimen, p.name)
        except Exception as e:
            log.warning("Failed to load regime model from %s: %r", p.name, e)

    # Load global fallback model: setup_classifier.pkl
    global_path = MODELS_DIR / "setup_classifier.pkl"
    global_meta = MODELS_DIR / "setup_classifier_meta.json"
    if global_path.exists():
        try:
            _GLOBAL_MODEL = joblib.load(global_path)
            log.info("Loaded global fallback classifier (%s)", global_path.name)
            if global_meta.exists():
                gm = json.loads(global_meta.read_text(encoding="utf-8"))
                raw = gm.get("feature_names") or []
                if isinstance(raw, (list, tuple)):
                    _GLOBAL_FEATURES = [str(x) for x in raw]
        except Exception as e:
            log.warning("Failed to load global fallback classifier: %r", e)


def _pick_model_for_regime(regime: str):
    """
    Return (model, feature_names, regime_key_used)
    """
    if regime in _REGIME_MODELS:
        return _REGIME_MODELS[regime], _REGIME_FEATURES.get(regime, []), regime

    low = (regime or "").lower()
    for rkey in _REGIME_MODELS:
        if rkey.lower() == low:
            return _REGIME_MODELS[rkey], _REGIME_FEATURES.get(rkey, []), rkey

    if _GLOBAL_MODEL is not None:
        return _GLOBAL_MODEL, _GLOBAL_FEATURES, "global"

    return None, [], None


def _maybe_debug(features: Dict[str, Any], used_regime: Optional[str], vec: List[float], has_model: bool) -> None:
    global _LAST_DEBUG_TS
    now = time.time()
    if now - _LAST_DEBUG_TS < 5.0:
        return
    _LAST_DEBUG_TS = now

    nonzero = sum(1 for x in vec if abs(float(x)) > 1e-12)
    log.info(
        "🧠 CLASSIFIER debug regime=%s used_model=%s vec_len=%s nonzero=%s atr_pct=%.4f vol_z=%.4f trend=%.4f",
        str(features.get("regime")),
        str(used_regime),
        int(len(vec)),
        int(nonzero),
        _safe_float(features.get("atr_pct"), 0.0),
        _safe_float(features.get("volume_zscore"), 0.0),
        _safe_float(features.get("trend_strength"), 0.0),
    )


def _classify_ai(signal: Dict[str, Any], strat_id: str) -> Dict[str, Any]:
    _load_models_once()

    features = _extract_live_features(signal)
    regime = str(features.get("regime") or "other")

    model_obj, feature_names, used_regime = _pick_model_for_regime(regime)

    # Always annotate
    features["used_regime_model"] = used_regime

    # No model: produce deterministic fallback score
    if model_obj is None:
        score = _fallback_score(features)
        min_score = _policy_min_ai_score(strat_id)
        features["min_ai_score"] = float(min_score)
        allow = bool(score >= min_score)
        reason = "fallback_score_ok" if allow else f"fallback_below_min_ai_score_{min_score:.3f}"
        vec = _vec_from_features(features, DEFAULT_FEATURE_ORDER)
        _maybe_debug(features, used_regime, vec, has_model=False)
        return {"allow": allow, "score": float(score), "reason": reason, "features": features}

    # Model exists: build vec in the exact expected feature order
    names = feature_names if isinstance(feature_names, list) and feature_names else DEFAULT_FEATURE_ORDER
    vec = _vec_from_features(features, names)

    # Inference
    try:
        probs = model_obj.predict_proba([vec])[0]
        score = float(probs[1]) if len(probs) > 1 else float(probs[0])
    except Exception as e:
        # If inference fails, still provide deterministic score so logs stay useful
        score = _fallback_score(features)
        min_score = _policy_min_ai_score(strat_id)
        features["min_ai_score"] = float(min_score)
        allow = bool(score >= min_score)
        reason = f"inference_error_fallback:{e}"
        _maybe_debug(features, used_regime, vec, has_model=True)
        return {"allow": allow, "score": float(score), "reason": reason, "features": features}

    # Policy threshold
    min_score = _policy_min_ai_score(strat_id)
    features["min_ai_score"] = float(min_score)

    allow = bool(score >= min_score)
    reason = "score_ok" if allow else f"below_min_ai_score_{min_score:.3f}"

    _maybe_debug(features, used_regime, vec, has_model=True)
    return {"allow": allow, "score": float(score), "reason": reason, "features": features}


# ──────────────────────────────────────────────────────────────────────────
# Public entrypoint with dual behavior
# ──────────────────────────────────────────────────────────────────────────
def classify(*args, **kwargs):
    """
    Public entry with dual behavior:

    1) NEW AI MODE (executor_v2):
        classify(signal: dict, strat_id: str) -> dict

    2) LEGACY LABEL MODE:
        classify(signal: dict, features: dict) -> str
    """
    if "strat_id" in kwargs or "strategy_id" in kwargs:
        signal = kwargs.get("signal") or (args[0] if args else {})
        strat_id = kwargs.get("strat_id") or kwargs.get("strategy_id")
        return _classify_ai(signal, str(strat_id))

    if len(args) == 2:
        a0, a1 = args
        if isinstance(a1, str):
            return _classify_ai(a0, a1)
        if isinstance(a1, dict):
            return _legacy_label(a0, a1)

    if len(args) == 1 and isinstance(args[0], dict):
        return "unknown"

    raise TypeError(
        "classify() expected (signal, strat_id:str) or (signal, features:dict); "
        f"got args={args}, kwargs={kwargs}"
    )
