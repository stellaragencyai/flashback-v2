#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Action Router v3.1 (Confidence-Gated + Perf-Store Gated)

Role
----
Tails AI Action Bus (state/ai_actions.jsonl) and processes validated actions for:
  • Logging (always)
  • Telegram notify (confidence-gated)
  • Future execution eligibility (just labeled for now)

Phase 3 covered
---------------
✅ Step 9: Wire Confidence → Action Router
✅ Step 12: Enforce Setup Performance Gate (UNPROVEN/PROBATION/APPROVED)

Compatibility
-------------
Accepts BOTH:
  A) envelope shape: {"label":..., "ts_ms":..., "action":{...}}
  B) flat shape     : {"account_label":..., "ts_ms":..., ...action_fields...}
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import orjson

# Core helpers
from app.core.flashback_common import (
    send_tg,
    record_heartbeat,
    alert_bot_error,
)

from app.core.ai_action_bus import ACTION_LOG_PATH

# Logging
try:
    from app.core.log import get_logger
except Exception:  # pragma: no cover
    import logging
    import sys

    def get_logger(name: str) -> "logging.Logger":  # type: ignore
        logger_ = logging.getLogger(name)
        if not logger_.handlers:
            handler = logging.StreamHandler(sys.stdout)
            fmt = logging.Formatter("%(asctime)s [%(levelname)s] [%(name)s] %(message)s")
            handler.setFormatter(fmt)
            logger_.addHandler(handler)
        logger_.setLevel(logging.INFO)
        return logger_


logger = get_logger("ai_action_router")

# Performance store (Phase 3)
try:
    from app.ai.setup_performance_store import get_stats, should_allow_action, DEFAULTS  # type: ignore
except Exception:
    get_stats = None  # type: ignore
    should_allow_action = None  # type: ignore
    DEFAULTS = {}  # type: ignore


# ---------------------------------------------------------------------------
# Env helpers
# ---------------------------------------------------------------------------

def _env_bool(name: str, default: str = "false") -> bool:
    raw = os.getenv(name, default).lower().strip()
    return raw in ("1", "true", "yes", "on")


def _env_int(name: str, default: str) -> int:
    try:
        return int(os.getenv(name, default).strip())
    except Exception:
        return int(default)


def _env_float(name: str, default: str) -> float:
    try:
        return float(os.getenv(name, default).strip())
    except Exception:
        return float(default)


ACCOUNT_LABEL = os.getenv("ACCOUNT_LABEL", "main").strip() or "main"
AI_ROUTER_ENABLED = _env_bool("AI_ROUTER_ENABLED", "true")
POLL_SECONDS = _env_int("AI_ROUTER_POLL_SECONDS", "2")
SEND_TG = _env_bool("AI_ROUTER_SEND_TG", "true")

# Optional debug logging (replaces the broken inline print spam)
DEBUG = _env_bool("AI_ROUTER_DEBUG", "false")

# Confidence thresholds (Step 9)
CONF_NOTIFY = _env_float("AI_ROUTER_CONF_NOTIFY", "0.4")
CONF_EXEC_ELIGIBLE = _env_float("AI_ROUTER_CONF_EXEC_ELIGIBLE", "0.7")

# Anti-spam throttle
MAX_TG_PER_MINUTE = _env_int("AI_ROUTER_MAX_TG_PER_MINUTE", "60")
_last_minute = int(time.time() // 60)
_sent_count = 0

# Action types we accept (loose)
_VALID_TYPES = {"open", "close", "reduce", "adjust_tp", "adjust_sl"}


# ---------------------------------------------------------------------------
# Compatibility: envelope OR flat action
# ---------------------------------------------------------------------------

def _normalize_to_envelope(obj: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Accept either:
      A) {"label":..., "ts_ms":..., "action":{...}}
      B) {"account_label":..., "ts_ms":..., ...action_fields...}

    Returns canonical envelope:
      {"label": <account_label>, "ts_ms": int, "action": { ... }}
    """
    if not isinstance(obj, dict):
        return None

    # A) already envelope
    if isinstance(obj.get("action"), dict):
        label = obj.get("label") or obj.get("account_label")
        ts = obj.get("ts_ms")
        if isinstance(label, str) and isinstance(ts, int):
            return {"label": label, "ts_ms": ts, "action": dict(obj["action"])}

    # B) flat action
    label = obj.get("account_label") or obj.get("label")
    ts = obj.get("ts_ms") or obj.get("ts")
    if not isinstance(label, str):
        return None
    try:
        ts_i = int(ts)
    except Exception:
        return None

    # Treat remaining fields as action dict
    act = dict(obj)
    act.pop("label", None)
    act.pop("account_label", None)
    act.pop("ts", None)
    act.pop("ts_ms", None)

    return {"label": str(label), "ts_ms": ts_i, "action": act}


def _validate_env(env: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(env, dict):
        return None
    if "action" not in env or not isinstance(env["action"], dict):
        return None

    label = env.get("label")
    if not isinstance(label, str):
        return None

    # Only process actions for THIS ACCOUNT_LABEL
    if label != ACCOUNT_LABEL:
        return None

    ts = env.get("ts_ms")
    if not isinstance(ts, int):
        return None

    return env


def _validate_action(act: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Validate an AI action dict (tolerant, but safe).
    """
    if not isinstance(act, dict):
        return None

    t = str(act.get("type", "")).lower().strip()
    if not t:
        t = str(act.get("action_type", "")).lower().strip()

    if t and t not in _VALID_TYPES:
        t = "unknown"

    symbol = act.get("symbol")
    if not isinstance(symbol, str) or not symbol.strip():
        return None
    symbol = symbol.strip()

    out: Dict[str, Any] = {
        "type": t,
        "symbol": symbol,
        "side": act.get("side"),
        "qty": act.get("qty"),
        "reason": act.get("reason", "unspecified"),
        "setup_fingerprint": act.get("setup_fingerprint"),
        "confidence": act.get("confidence") or act.get("confidence_score"),
        "extra": act.get("extra") if isinstance(act.get("extra"), dict) else {},
        "raw": act,
    }

    # Sanitize side
    if out["side"] is not None:
        s = str(out["side"]).lower().strip()
        if s not in ("buy", "sell", "long", "short"):
            return None
        out["side"] = s

    # Sanitize qty
    if out["qty"] is not None:
        try:
            q = float(out["qty"])
            if q < 0:
                return None
            out["qty"] = q
        except Exception:
            return None

    # Confidence sanitize
    if out["confidence"] is not None:
        try:
            out["confidence"] = float(out["confidence"])
        except Exception:
            out["confidence"] = None

    # Fingerprint sanitize
    if out["setup_fingerprint"] is not None:
        if not isinstance(out["setup_fingerprint"], str) or not out["setup_fingerprint"].strip():
            out["setup_fingerprint"] = None
        else:
            out["setup_fingerprint"] = out["setup_fingerprint"].strip()

    return out


# ---------------------------------------------------------------------------
# File tailing
# ---------------------------------------------------------------------------

def _iter_new_objects(path: Path, offset: int) -> Tuple[int, List[Dict[str, Any]]]:
    """
    Safe tailing with truncation detection.
    Reads JSONL objects.
    """
    if not path.exists():
        return offset, []

    try:
        size = path.stat().st_size
    except Exception:
        return offset, []

    if offset > size:
        logger.warning("ai_action_router: file truncated; resetting offset to 0")
        offset = 0

    objs: List[Dict[str, Any]] = []
    try:
        with path.open("rb") as f:
            f.seek(offset)
            for line in f:
                offset += len(line)
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = orjson.loads(line)
                    if isinstance(obj, dict):
                        objs.append(obj)
                except Exception:
                    continue
    except Exception as e:
        alert_bot_error("ai_action_router", f"read error: {e}", "ERROR")
        return offset, []

    return offset, objs


# ---------------------------------------------------------------------------
# Thresholds loader (disk store → fallback defaults, with ENV overrides)
# ---------------------------------------------------------------------------

def _load_thresholds() -> Dict[str, Any]:
    """
    Load thresholds from disk-backed perf store if present:
      state/ai_perf/setup_perf.json

    Merge order (intentional):
      1) setup_performance_store.DEFAULTS (baseline)
      2) disk store thresholds override defaults
      3) ENV overrides ALWAYS win (ops/testing control)
    """
    th: Dict[str, Any] = {}

    # 1) baseline defaults
    if isinstance(DEFAULTS, dict) and DEFAULTS:
        th.update(DEFAULTS)
    else:
        th.update({
            "min_trades": 20,
            "probation_trades": 50,
            "min_avg_r_for_approval": 0.15,
            "max_stdev_r_for_approval": 2.5,
            "max_missing_r_frac": 0.40,
            "recency_halflife_days": 7.0,
            "confidence_notify": 0.4,
            "confidence_execute": 0.7,
        })

    # 2) disk thresholds override defaults (if present)
    store_path = Path("state") / "ai_perf" / "setup_perf.json"
    try:
        if store_path.exists():
            obj = orjson.loads(store_path.read_bytes())
            if isinstance(obj, dict):
                disk_th = obj.get("thresholds")
                if isinstance(disk_th, dict) and disk_th:
                    for k, v in disk_th.items():
                        th[k] = v
    except Exception:
        pass

    # 3) ENV overrides ALWAYS win
    th["confidence_notify"] = float(CONF_NOTIFY)
    th["confidence_execute"] = float(CONF_EXEC_ELIGIBLE)

    return th


# ---------------------------------------------------------------------------
# Decision: store-gate first, then confidence tiers
# ---------------------------------------------------------------------------

def _gate_action_with_store(action: Dict[str, Any]) -> Tuple[str, float, str]:
    """
    Returns (decision, confidence, reason)
    decision ∈ {LOG_ONLY, NOTIFY, EXEC_ELIGIBLE}
    """
    fp = action.get("setup_fingerprint")
    raw_conf = action.get("confidence")
    raw_conf = float(raw_conf) if isinstance(raw_conf, (int, float)) else None

    # If perf store isn't available, fallback to raw confidence only (Step 9 behavior)
    if get_stats is None or should_allow_action is None:
        if raw_conf is None:
            return "LOG_ONLY", 0.0, "NO_STORE_NO_CONF"
        if raw_conf >= CONF_EXEC_ELIGIBLE:
            return "EXEC_ELIGIBLE", raw_conf, f"HIGH_CONF (conf={raw_conf:.2f})"
        if raw_conf >= CONF_NOTIFY:
            return "NOTIFY", raw_conf, f"OK_CONF (conf={raw_conf:.2f})"
        return "LOG_ONLY", raw_conf, f"LOW_CONF (conf={raw_conf:.2f})"

    thresholds = _load_thresholds()

    # Store-based gating requires fingerprint. No fp = no learning identity = no trust.
    if not fp:
        c = raw_conf if raw_conf is not None else 0.0
        return "LOG_ONLY", c, "MISSING_FP_UNTRACKABLE"

    stats = get_stats(fp)
    if not stats:
        c = raw_conf if raw_conf is not None else 0.0
        return "LOG_ONLY", c, "NO_STATS_UNPROVEN"

    allow, reason, conf2, status = should_allow_action(stats, thresholds)

    conf2f = float(conf2) if conf2 is not None else 0.0
    reason = str(reason) if reason is not None else "UNKNOWN_REASON"
    status = str(status) if status is not None else "UNKNOWN_STATUS"

    if not allow:
        return "LOG_ONLY", conf2f, f"{status}: {reason}"

    # Allowed: tier by computed confidence
    if conf2f >= float(thresholds.get("confidence_execute", CONF_EXEC_ELIGIBLE)):
        return "EXEC_ELIGIBLE", conf2f, f"{status}: {reason}"
    if conf2f >= float(thresholds.get("confidence_notify", CONF_NOTIFY)):
        return "NOTIFY", conf2f, f"{status}: {reason}"

    return "LOG_ONLY", conf2f, f"{status}: LOW_CONF (conf={conf2f:.2f})"


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def _fmt_env_to_text(
    env: Dict[str, Any],
    act: Dict[str, Any],
    decision: str,
    confidence: float,
    decision_reason: str,
) -> str:
    lines: List[str] = []
    lines.append("🤖 AI Action (DRY-RUN)")
    lines.append(f"• label      : {env.get('label')}")
    lines.append(f"• decision   : {decision}")
    lines.append(f"• confidence : {confidence:.2f}")
    lines.append(f"• gate_reason: {decision_reason}")

    lines.append(f"• type       : {act.get('type')}")
    lines.append(f"• symbol     : {act.get('symbol')}")
    lines.append(f"• side       : {act.get('side')}")
    lines.append(f"• qty        : {act.get('qty')}")
    lines.append(f"• reason     : {act.get('reason')}")

    fp = act.get("setup_fingerprint")
    if fp:
        lines.append(f"• fingerprint: {fp[:12]}…")

    extra = act.get("extra") or {}
    if isinstance(extra, dict) and extra:
        kv = ", ".join(f"{k}={extra[k]}" for k in list(extra.keys())[:5])
        lines.append(f"• extra      : {kv}")

    return "\n".join(lines)


def _tg_throttle() -> bool:
    global _sent_count, _last_minute
    now_min = int(time.time() // 60)
    if now_min != _last_minute:
        _last_minute = now_min
        _sent_count = 0
    if _sent_count >= MAX_TG_PER_MINUTE:
        return False
    _sent_count += 1
    return True


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def loop() -> None:
    if not AI_ROUTER_ENABLED:
        logger.warning("AI Action Router disabled. Exiting.")
        return

    logger.info(
        "AI Action Router starting (label=%s, poll=%s, tg=%s, conf_notify=%.2f, conf_exec=%.2f, debug=%s)",
        ACCOUNT_LABEL, POLL_SECONDS, SEND_TG, CONF_NOTIFY, CONF_EXEC_ELIGIBLE, DEBUG
    )

    # Start at EOF to avoid spam
    try:
        offset = ACTION_LOG_PATH.stat().st_size if ACTION_LOG_PATH.exists() else 0
    except Exception:
        offset = 0

    # Startup notice (only if TG enabled and throttle allows)
    if SEND_TG and _tg_throttle():
        try:
            send_tg(f"📡 AI Action Router online for {ACCOUNT_LABEL} (store+confidence gated)")
        except Exception:
            pass

    while True:
        record_heartbeat("ai_action_router")

        try:
            offset, objs = _iter_new_objects(ACTION_LOG_PATH, offset)

            if DEBUG:
                logger.info("[DBG] poll: offset=%s objs=%s path=%s", offset, len(objs), str(ACTION_LOG_PATH))

            for obj in objs:
                env = _normalize_to_envelope(obj)
                if env is None:
                    continue

                if DEBUG:
                    try:
                        act0 = env.get("action") if isinstance(env, dict) else None
                        fp0 = act0.get("setup_fingerprint") if isinstance(act0, dict) else None
                        logger.info("[DBG] obj: label=%s fp=%s", env.get("label"), fp0)
                    except Exception:
                        pass

                venv = _validate_env(env)
                if venv is None:
                    continue

                vact = _validate_action(venv["action"])
                if vact is None:
                    continue

                decision, conf, gate_reason = _gate_action_with_store(vact)

                text = _fmt_env_to_text(venv, vact, decision, conf, gate_reason)
                logger.info(text)

                # TG rules
                if not SEND_TG:
                    continue
                if decision == "LOG_ONLY":
                    continue
                if not _tg_throttle():
                    continue

                try:
                    send_tg(text)
                except Exception as e:
                    alert_bot_error("ai_action_router", f"tg error: {e}", "WARN")

        except Exception as e:
            alert_bot_error("ai_action_router", f"loop error: {e}", "ERROR")

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    loop()
