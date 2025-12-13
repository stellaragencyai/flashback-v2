#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Setup Performance Store v1.5 (Disk-backed aggregation + Rejection Memory + Learn-R + weighted stdev)

v1.5:
- Uses explicit terminal truth when present:
    * evt.extra.is_terminal == True  => terminal trainable (unless synthetic disallowed)
    * evt.extra.final_status == "FILL_EVENT" => not trainable
  Fallback inference remains but is now secondary.
- Learn-R normalization policy (mode-aware):
    PAPER       clamp [-2,2],   weight 0.05
    LIVE_CANARY clamp [-5,5],   weight 0.50
    LIVE_FULL   clamp [-10,10], weight 1.00
    LIVE        clamp [-10,10], weight 1.00
- Computes weighted mean AND weighted stdev (consistent).
- Stores raw_r_values + learn_r_values (debug, capped).

Reads:
  state/ai_events/outcomes.jsonl
Writes:
  state/ai_perf/setup_perf.json
"""

from __future__ import annotations

import math
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import orjson


try:
    from app.core.config import settings  # type: ignore
    ROOT: Path = settings.ROOT  # type: ignore[attr-defined]
except Exception:
    ROOT = Path(__file__).resolve().parents[2]

STATE_DIR = ROOT / "state"
AI_EVENTS_DIR = STATE_DIR / "ai_events"
OUTCOMES_PATH = AI_EVENTS_DIR / "outcomes.jsonl"

PERF_DIR = STATE_DIR / "ai_perf"
PERF_DIR.mkdir(parents=True, exist_ok=True)

STORE_PATH = PERF_DIR / "setup_perf.json"
CURSOR_PATH = PERF_DIR / "setup_perf.cursor"

DEFAULTS = {
    "min_trades": 20,
    "probation_trades": 50,
    "min_avg_r_for_approval": 0.15,
    "max_stdev_r_for_approval": 2.5,
    "max_missing_r_frac": 0.40,
    "recency_halflife_days": 7.0,
    "confidence_notify": 0.4,
    "confidence_execute": 0.7,
}

ALLOW_SYNTHETIC_TERMINALS = os.getenv("PERF_ALLOW_SYNTHETIC_TERMINALS", "false").strip().lower() in ("1", "true", "yes")

try:
    PERF_MAX_ABS_RAW_R = float(os.getenv("PERF_MAX_ABS_RAW_R", "1000000"))
except Exception:
    PERF_MAX_ABS_RAW_R = 1_000_000.0

try:
    from app.core.log import get_logger  # type: ignore
except Exception:  # pragma: no cover
    import logging
    def get_logger(name: str) -> "logging.Logger":  # type: ignore
        logger_ = logging.getLogger(name)
        if not logger_.handlers:
            h = logging.StreamHandler()
            fmt = logging.Formatter("%(asctime)s [%(levelname)s] [%(name)s] %(message)s")
            h.setFormatter(fmt)
            logger_.addHandler(h)
        logger_.setLevel(logging.INFO)
        return logger_

log = get_logger("setup_performance_store")


def _now_ms() -> int:
    return int(time.time() * 1000)


def _load_cursor() -> int:
    if not CURSOR_PATH.exists():
        return 0
    try:
        return int(CURSOR_PATH.read_text(encoding="utf-8").strip() or "0")
    except Exception:
        return 0


def _save_cursor(pos: int) -> None:
    try:
        CURSOR_PATH.write_text(str(pos), encoding="utf-8")
    except Exception as e:
        log.warning("failed to save cursor=%s: %r", pos, e)


def _empty_store() -> Dict[str, Any]:
    return {
        "version": 1,
        "updated_ms": _now_ms(),
        "thresholds": dict(DEFAULTS),
        "setups": {},
    }


def _load_store() -> Dict[str, Any]:
    if not STORE_PATH.exists():
        return _empty_store()

    try:
        raw = STORE_PATH.read_bytes()
        obj = orjson.loads(raw)
        if not isinstance(obj, dict):
            raise ValueError("store not a dict")
        obj.setdefault("version", 1)
        th = obj.get("thresholds")
        if not isinstance(th, dict):
            obj["thresholds"] = dict(DEFAULTS)
        else:
            for k, v in DEFAULTS.items():
                th.setdefault(k, v)
        setups = obj.get("setups")
        if not isinstance(setups, dict):
            obj["setups"] = {}
        return obj
    except Exception as e:
        log.warning("Failed to load %s; returning empty store. err=%r", STORE_PATH, e)
        return _empty_store()


def _save_store(store: Dict[str, Any]) -> None:
    store["updated_ms"] = _now_ms()
    try:
        STORE_PATH.write_bytes(orjson.dumps(store, option=orjson.OPT_INDENT_2))
    except Exception as e:
        log.warning("failed to save store: %r", e)


def _median(xs: List[float]) -> Optional[float]:
    if not xs:
        return None
    ys = sorted(xs)
    n = len(ys)
    mid = n // 2
    if n % 2 == 1:
        return ys[mid]
    return 0.5 * (ys[mid - 1] + ys[mid])


def _max_drawdown_from_cum_series(rs: List[float]) -> Optional[float]:
    if not rs:
        return None
    peak = 0.0
    cum = 0.0
    max_dd = 0.0
    for r in rs:
        cum += r
        peak = max(peak, cum)
        dd = peak - cum
        max_dd = max(max_dd, dd)
    return max_dd


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _extract_mode(evt: Dict[str, Any]) -> str:
    setup = evt.get("setup")
    if isinstance(setup, dict):
        payload = setup.get("payload")
        if isinstance(payload, dict):
            extra = payload.get("extra")
            if isinstance(extra, dict):
                mode = extra.get("mode")
                if isinstance(mode, str) and mode.strip():
                    return mode.strip().upper()

    extra2 = evt.get("extra")
    if isinstance(extra2, dict):
        mode2 = extra2.get("mode")
        if isinstance(mode2, str) and mode2.strip():
            return mode2.strip().upper()

    return "LIVE"


def _normalize_r(raw_r: Optional[float], mode: str) -> Tuple[Optional[float], float]:
    if raw_r is None:
        return None, 0.0

    m = (mode or "LIVE").strip().upper()

    if m == "PAPER":
        return _clamp(float(raw_r), -2.0, 2.0), 0.05
    if m in ("LIVE_CANARY", "CANARY"):
        return _clamp(float(raw_r), -5.0, 5.0), 0.50
    if m in ("LIVE_FULL", "FULL"):
        return _clamp(float(raw_r), -10.0, 10.0), 1.00

    return _clamp(float(raw_r), -10.0, 10.0), 1.00


def _is_synthetic_terminal(evt: Dict[str, Any]) -> bool:
    extra = evt.get("extra")
    if isinstance(extra, dict):
        schema = extra.get("schema_version") or ""
        if isinstance(schema, str) and schema.startswith("synthetic_terminal"):
            return True
    return False


def _extract_is_terminal_flag(evt: Dict[str, Any]) -> Optional[bool]:
    extra = evt.get("extra")
    if isinstance(extra, dict):
        v = extra.get("is_terminal")
        if isinstance(v, bool):
            return v
    return None


def _extract_final_status(evt: Dict[str, Any]) -> Optional[str]:
    extra = evt.get("extra")
    if isinstance(extra, dict):
        fs = extra.get("final_status")
        if isinstance(fs, str) and fs.strip():
            return fs.strip().upper()

    payload = evt.get("payload")
    if isinstance(payload, dict):
        extra2 = payload.get("extra")
        if isinstance(extra2, dict):
            fs2 = extra2.get("final_status")
            if isinstance(fs2, str) and fs2.strip():
                return fs2.strip().upper()

    return None


def _extract_r_any(evt: Dict[str, Any]) -> Optional[float]:
    stats = evt.get("stats")
    if isinstance(stats, dict):
        v = stats.get("r_multiple")
        if v is not None:
            try:
                return float(v)
            except Exception:
                pass

    payload = evt.get("payload")
    if isinstance(payload, dict):
        v = payload.get("r_multiple")
        if v is not None:
            try:
                return float(v)
            except Exception:
                pass

    extra = evt.get("extra")
    if isinstance(extra, dict):
        v = extra.get("r_multiple")
        if v is not None:
            try:
                return float(v)
            except Exception:
                pass

    out = evt.get("outcome")
    if isinstance(out, dict):
        op = out.get("payload")
        if isinstance(op, dict):
            v = op.get("r_multiple")
            if v is not None:
                try:
                    return float(v)
                except Exception:
                    pass
        os_ = out.get("stats")
        if isinstance(os_, dict):
            v = os_.get("r_multiple")
            if v is not None:
                try:
                    return float(v)
                except Exception:
                    pass

    return None


def _extract_learn_fields(evt: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], float]:
    raw_r = _extract_r_any(evt)
    if raw_r is not None and (math.isnan(raw_r) or math.isinf(raw_r) or abs(raw_r) > PERF_MAX_ABS_RAW_R):
        raw_r = None

    # Prefer upstream learn fields if present
    stats = evt.get("stats")
    if isinstance(stats, dict):
        lr = stats.get("learn_r_multiple")
        lw = stats.get("learn_weight")
        rr = stats.get("raw_r_multiple")
        if rr is not None:
            try:
                raw_r = float(rr)
            except Exception:
                pass
        if lr is not None:
            try:
                learn_r = float(lr)
            except Exception:
                learn_r = None
            try:
                w = float(lw) if lw is not None else 0.0
            except Exception:
                w = 0.0
            return raw_r, learn_r, max(0.0, w)

    extra = evt.get("extra")
    if isinstance(extra, dict):
        lr = extra.get("learn_r_multiple")
        lw = extra.get("learn_weight")
        rr = extra.get("raw_r_multiple")
        if rr is not None:
            try:
                raw_r = float(rr)
            except Exception:
                pass
        if lr is not None:
            try:
                learn_r = float(lr)
            except Exception:
                learn_r = None
            try:
                w = float(lw) if lw is not None else 0.0
            except Exception:
                w = 0.0
            return raw_r, learn_r, max(0.0, w)

    mode = _extract_mode(evt)
    learn_r, w = _normalize_r(raw_r, mode)
    return raw_r, learn_r, w


def _is_terminal_trainable(evt: Dict[str, Any]) -> bool:
    is_term = _extract_is_terminal_flag(evt)
    if is_term is False:
        return False

    fs = _extract_final_status(evt)
    if fs == "FILL_EVENT":
        return False

    if _is_synthetic_terminal(evt) and not ALLOW_SYNTHETIC_TERMINALS:
        return False

    et = evt.get("event_type") or evt.get("type")
    if et not in ("outcome_enriched", "outcome_record"):
        return False

    # Explicit terminal wins
    if is_term is True:
        return True

    # Fallback inference: if r exists, treat as terminal-ish
    r = _extract_r_any(evt)
    return r is not None


def _extract_fp(evt: Dict[str, Any]) -> Optional[str]:
    setup = evt.get("setup")
    if isinstance(setup, dict):
        payload = setup.get("payload")
        if isinstance(payload, dict):
            feats = payload.get("features")
            if isinstance(feats, dict):
                fp = feats.get("setup_fingerprint")
                if isinstance(fp, str) and fp.strip():
                    return fp.strip()

    fp2 = evt.get("setup_fingerprint")
    if isinstance(fp2, str) and fp2.strip():
        return fp2.strip()

    extra = evt.get("extra")
    if isinstance(extra, dict):
        fp3 = extra.get("setup_fingerprint")
        if isinstance(fp3, str) and fp3.strip():
            return fp3.strip()

    return None


def _extract_identity(evt: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "symbol": evt.get("symbol"),
        "timeframe": evt.get("timeframe"),
        "account_label": evt.get("account_label"),
        "strategy_name": evt.get("strategy_name") or evt.get("strategy"),
    }

    setup = evt.get("setup")
    if isinstance(setup, dict):
        out["symbol"] = out["symbol"] or setup.get("symbol")
        out["timeframe"] = out["timeframe"] or setup.get("timeframe")
        out["account_label"] = out["account_label"] or setup.get("account_label")
        out["strategy_name"] = out["strategy_name"] or setup.get("strategy_name") or setup.get("strategy")

        payload = setup.get("payload")
        if isinstance(payload, dict):
            feats = payload.get("features")
            if isinstance(feats, dict):
                sff = feats.get("setup_fingerprint_features")
                if isinstance(sff, dict):
                    out["atr_bucket"] = sff.get("atr_bucket")
                    out["volz_bucket"] = sff.get("volz_bucket")
                    out["trend_flag"] = sff.get("trend_flag")
                    out["range_flag"] = sff.get("range_flag")
                    out["vol_flag"] = sff.get("vol_flag")

    return out


def _weighted_stdev(samples: List[Tuple[float, float]], mean: float) -> Optional[float]:
    """
    samples: [(r, w), ...]
    """
    if not samples:
        return None
    sum_w = sum(w for _, w in samples)
    if sum_w <= 0:
        return None
    var = sum(w * (r - mean) ** 2 for r, w in samples) / sum_w
    return math.sqrt(var)


def update_from_outcomes(max_lines: int = 50_000) -> Dict[str, Any]:
    store = _load_store()
    thresholds = store.get("thresholds") or {}
    if not isinstance(thresholds, dict):
        thresholds = dict(DEFAULTS)
        store["thresholds"] = thresholds

    if not OUTCOMES_PATH.exists():
        _save_store(store)
        return store

    pos = _load_cursor()
    processed = 0
    updated = 0
    skipped_nonterminal = 0
    skipped_missing_learn_r = 0

    try:
        size = OUTCOMES_PATH.stat().st_size
        if pos > size:
            pos = 0
            _save_cursor(pos)

        with OUTCOMES_PATH.open("rb") as f:
            f.seek(pos)
            for raw in f:
                pos = f.tell()
                processed += 1
                if processed > max_lines:
                    break

                line = raw.strip()
                if not line:
                    continue

                try:
                    evt = orjson.loads(line)
                except Exception:
                    continue
                if not isinstance(evt, dict):
                    continue

                et = evt.get("event_type") or evt.get("type")
                if et not in ("outcome_enriched", "outcome_record"):
                    continue

                if not _is_terminal_trainable(evt):
                    skipped_nonterminal += 1
                    continue

                fp = _extract_fp(evt)
                if not fp:
                    continue

                raw_r, learn_r, learn_w = _extract_learn_fields(evt)
                if learn_r is None or learn_w <= 0:
                    skipped_missing_learn_r += 1
                    continue

                ts = evt.get("ts_ms") or evt.get("ts") or _now_ms()
                try:
                    ts_i = int(ts)
                except Exception:
                    ts_i = _now_ms()

                setups = store.get("setups")
                if not isinstance(setups, dict):
                    setups = {}
                    store["setups"] = setups

                st = setups.get(fp)
                if not isinstance(st, dict):
                    st = {
                        "fingerprint": fp,
                        "identity": _extract_identity(evt),

                        "count": 0,
                        "count_terminal": 0,

                        "sum_w": 0.0,
                        "sum_wr": 0.0,
                        "win_w": 0.0,
                        "loss_w": 0.0,

                        "count_with_r": 0.0,
                        "count_missing_r": 0,

                        "raw_r_values": [],
                        "learn_r_values": [],
                        "r_values": [],  # legacy mirror of learn_r

                        "avg_r": None,
                        "median_r": None,
                        "stdev_r": None,
                        "min_r": None,
                        "max_r": None,
                        "max_drawdown_r": None,

                        "last_seen_ts": ts_i,
                        "first_seen_ts": ts_i,

                        "status": "UNPROVEN",
                        "confidence_score": 0.0,

                        "rejection_reason": None,
                        "rejection_ts_ms": None,
                        "last_rejected_ts_ms": None,
                        "rejection_count": 0,
                    }
                    setups[fp] = st

                st["count"] = int(st.get("count") or 0) + 1
                st["count_terminal"] = int(st.get("count_terminal") or 0) + 1
                st["last_seen_ts"] = max(int(st.get("last_seen_ts") or ts_i), ts_i)
                st["first_seen_ts"] = min(int(st.get("first_seen_ts") or ts_i), ts_i)

                if not st.get("identity"):
                    st["identity"] = _extract_identity(evt)

                rr = float(learn_r)
                w = float(learn_w)

                st["sum_w"] = float(st.get("sum_w") or 0.0) + w
                st["sum_wr"] = float(st.get("sum_wr") or 0.0) + (rr * w)

                st["count_with_r"] = float(st.get("count_with_r") or 0.0) + w

                if rr > 0:
                    st["win_w"] = float(st.get("win_w") or 0.0) + w
                else:
                    st["loss_w"] = float(st.get("loss_w") or 0.0) + w

                # Debug arrays (capped)
                rawvals = st.get("raw_r_values")
                if not isinstance(rawvals, list):
                    rawvals = []
                    st["raw_r_values"] = rawvals
                if raw_r is not None:
                    rawvals.append(float(raw_r))
                    if len(rawvals) > 500:
                        rawvals[:] = rawvals[-500:]

                lvals = st.get("learn_r_values")
                if not isinstance(lvals, list):
                    lvals = []
                    st["learn_r_values"] = lvals
                lvals.append(rr)
                if len(lvals) > 500:
                    lvals[:] = lvals[-500:]

                # legacy mirror
                rvals = st.get("r_values")
                if not isinstance(rvals, list):
                    rvals = []
                    st["r_values"] = rvals
                rvals.append(rr)
                if len(rvals) > 500:
                    rvals[:] = rvals[-500:]

                st["min_r"] = rr if st.get("min_r") is None else min(float(st["min_r"]), rr)
                st["max_r"] = rr if st.get("max_r") is None else max(float(st["max_r"]), rr)

                _recompute_stats_and_gates(st, thresholds)
                updated += 1

    except Exception as e:
        log.warning("update_from_outcomes error: %r", e)

    _save_cursor(pos)
    store["last_update"] = {
        "processed_lines": processed,
        "updated_setups": updated,
        "cursor": pos,
        "skipped_nonterminal": skipped_nonterminal,
        "skipped_missing_learn_r": skipped_missing_learn_r,
        "allow_synthetic_terminals": ALLOW_SYNTHETIC_TERMINALS,
        "max_abs_raw_r": PERF_MAX_ABS_RAW_R,
    }
    _save_store(store)
    return store


def gate_setup(stats: Dict[str, Any], thresholds: Dict[str, Any]) -> str:
    min_trades = int(thresholds.get("min_trades", DEFAULTS["min_trades"]))
    probation_trades = int(thresholds.get("probation_trades", DEFAULTS["probation_trades"]))

    n_eff = float(stats.get("count_with_r") or 0.0)
    avg_r = stats.get("avg_r")
    stdev_r = stats.get("stdev_r")

    if n_eff < float(min_trades):
        return "UNPROVEN"

    if n_eff < float(probation_trades):
        return "PROBATION"

    min_avg_r = float(thresholds.get("min_avg_r_for_approval", DEFAULTS["min_avg_r_for_approval"]))
    max_stdev = float(thresholds.get("max_stdev_r_for_approval", DEFAULTS["max_stdev_r_for_approval"]))

    if avg_r is None:
        return "PROBATION"

    if float(avg_r) < min_avg_r:
        return "PROBATION"

    if stdev_r is not None and float(stdev_r) > max_stdev:
        return "PROBATION"

    return "APPROVED"


def score_confidence(stats: Dict[str, Any], thresholds: Dict[str, Any]) -> float:
    min_trades = int(thresholds.get("min_trades", DEFAULTS["min_trades"]))
    probation_trades = int(thresholds.get("probation_trades", DEFAULTS["probation_trades"]))
    min_avg_r = float(thresholds.get("min_avg_r_for_approval", DEFAULTS["min_avg_r_for_approval"]))
    recency_halflife_days = float(thresholds.get("recency_halflife_days", DEFAULTS["recency_halflife_days"]))
    max_missing_r_frac = float(thresholds.get("max_missing_r_frac", DEFAULTS["max_missing_r_frac"]))

    n_eff = float(stats.get("count_with_r") or 0.0)
    total = int(stats.get("count") or 0)
    avg_r = float(stats.get("avg_r") or 0.0)
    stdev_r = float(stats.get("stdev_r") or 0.0)

    if n_eff <= 0:
        sample_score = 0.0
    else:
        if n_eff < float(min_trades):
            sample_score = n_eff / float(max(1, min_trades))
        else:
            sample_score = min(1.0, n_eff / float(max(1, probation_trades)))

    edge_score = 1.0 / (1.0 + math.exp(-(avg_r - min_avg_r)))
    vol_pen = 1.0 / (1.0 + stdev_r)

    last_ts = int(stats.get("last_seen_ts") or _now_ms())
    age_days = max(0.0, (_now_ms() - last_ts) / 86_400_000.0)
    recency = math.pow(0.5, age_days / max(0.1, recency_halflife_days))

    missing = int(stats.get("count_missing_r") or 0)
    missing_frac = (missing / max(1, total)) if total > 0 else 1.0
    miss_pen = 1.0
    if missing_frac > max_missing_r_frac:
        miss_pen = max(0.0, 1.0 - (missing_frac - max_missing_r_frac) * 2.0)

    score = sample_score * edge_score * vol_pen * recency * miss_pen
    return float(max(0.0, min(1.0, score)))


def should_allow_action(stats: Dict[str, Any], thresholds: Dict[str, Any]) -> Tuple[bool, str, float, str]:
    status = gate_setup(stats, thresholds)
    conf = score_confidence(stats, thresholds)

    if status == "UNPROVEN":
        return False, "UNPROVEN (insufficient outcomes)", conf, status

    if stats.get("avg_r") is not None and float(stats["avg_r"]) < 0:
        return False, "NEG_EDGE (avg_r < 0)", conf, status

    total = int(stats.get("count") or 0)
    missing = int(stats.get("count_missing_r") or 0)
    missing_frac = (missing / max(1, total)) if total > 0 else 1.0
    max_missing_r_frac = float(thresholds.get("max_missing_r_frac", DEFAULTS["max_missing_r_frac"]))
    if missing_frac > max_missing_r_frac:
        return False, f"DATA_QUALITY (missing_r_frac={missing_frac:.2f})", conf, status

    if status == "PROBATION":
        if conf < float(thresholds.get("confidence_notify", DEFAULTS["confidence_notify"])):
            return False, f"PROBATION_LOW_CONF (conf={conf:.2f})", conf, status
        return True, f"PROBATION_OK (conf={conf:.2f})", conf, status

    return True, f"APPROVED (conf={conf:.2f})", conf, status


def _apply_rejection_memory(st: Dict[str, Any], allow: bool, reason: str) -> None:
    now = _now_ms()
    st.setdefault("rejection_reason", None)
    st.setdefault("rejection_ts_ms", None)
    st.setdefault("last_rejected_ts_ms", None)
    st.setdefault("rejection_count", 0)

    if not allow:
        st["rejection_reason"] = reason
        if st.get("rejection_ts_ms") is None:
            st["rejection_ts_ms"] = now
        st["last_rejected_ts_ms"] = now
        st["rejection_count"] = int(st.get("rejection_count") or 0) + 1
    else:
        st["rejection_reason"] = None


def _recompute_stats_and_gates(st: Dict[str, Any], thresholds: Dict[str, Any]) -> None:
    lvals = st.get("learn_r_values")
    if not isinstance(lvals, list):
        lvals = []
        st["learn_r_values"] = lvals

    sum_w = float(st.get("sum_w") or 0.0)
    sum_wr = float(st.get("sum_wr") or 0.0)

    if sum_w > 0:
        mean = sum_wr / sum_w
        st["avg_r"] = mean
        st["median_r"] = _median([float(x) for x in lvals if isinstance(x, (int, float))])
        # Weighted stdev using (learn_r, weight) pairs
        # We don't store per-sample weights in arrays, so we approximate by treating all
        # samples as equal for stdev only if you need exactness you must store per-sample weights.
        # Here we do the consistent thing: derive weights from mode only when events are processed.
        # So we store a parallel weights list during processing via st["_w_values"].
        wvals = st.get("_w_values")
        if isinstance(wvals, list) and len(wvals) == len(lvals):
            pairs = [(float(r), float(w)) for r, w in zip(lvals, wvals)]
            st["stdev_r"] = _weighted_stdev(pairs, mean)
        else:
            # Fallback: unweighted stdev if weights list missing (should not happen after v1.5 runs clean)
            if len(lvals) >= 2:
                m = sum(float(x) for x in lvals) / len(lvals)
                var = sum((float(x) - m) ** 2 for x in lvals) / (len(lvals) - 1)
                st["stdev_r"] = math.sqrt(var)
            else:
                st["stdev_r"] = None

        st["max_drawdown_r"] = _max_drawdown_from_cum_series([float(x) for x in lvals if isinstance(x, (int, float))])
    else:
        st["avg_r"] = None
        st["median_r"] = None
        st["stdev_r"] = None
        st["max_drawdown_r"] = None

    st["status"] = gate_setup(st, thresholds)
    st["confidence_score"] = score_confidence(st, thresholds)

    allow, reason, _, _ = should_allow_action(st, thresholds)
    _apply_rejection_memory(st, allow, reason)


def main() -> None:
    log.info(
        "Updating setup performance store from outcomes: %s (allow_synth=%s)",
        OUTCOMES_PATH, ALLOW_SYNTHETIC_TERMINALS
    )
    store = update_from_outcomes()
    setups = store.get("setups", {})
    n = len(setups) if isinstance(setups, dict) else 0
    log.info("Updated store. setups=%d, store=%s", n, STORE_PATH)


if __name__ == "__main__":
    main()
