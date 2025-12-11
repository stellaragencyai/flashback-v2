#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Action Router v2.8 (Hardened DRY-RUN Router)

Role
----
Tails AI Action Bus (state/ai_actions.jsonl) and processes validated
action envelopes for DRY-RUN inspection, logging, and optional TG send.

This hardened version includes:
  • Envelope schema validation
  • Action schema validation
  • ACCOUNT_LABEL filtering
  • Drift-safe tailing (file truncation recovery)
  • Anti-spam throttling for Telegram
  • Sanitized formatting
  • Crash-proof loop with alert_bot_error on anomalies

No live orders are placed here.
"""

from __future__ import annotations
import os, time
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional


import orjson

# Logging
try:
    from app.core.log import get_logger
except Exception:  # pragma: no cover
    import logging, sys

    def get_logger(name: str) -> "logging.Logger":  # type: ignore
        logger_ = logging.getLogger(name)
        if not logger_.handlers:
            handler = logging.StreamHandler(sys.stdout)
            fmt = logging.Formatter(
                "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
            )
            handler.setFormatter(fmt)
            logger_.addHandler(handler)
        logger_.setLevel(logging.INFO)
        return logger_

logger = get_logger("ai_action_router")

# Core helpers
from app.core.flashback_common import (
    send_tg,
    record_heartbeat,
    alert_bot_error,
)

from app.core.ai_action_bus import ACTION_LOG_PATH

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

ACCOUNT_LABEL = os.getenv("ACCOUNT_LABEL", "main").strip() or "main"
AI_ROUTER_ENABLED = _env_bool("AI_ROUTER_ENABLED", "true")
POLL_SECONDS = _env_int("AI_ROUTER_POLL_SECONDS", "2")
SEND_TG = _env_bool("AI_ROUTER_SEND_TG", "true")

# Anti-spam throttle
MAX_TG_PER_MINUTE = 60
_last_minute = int(time.time() // 60)
_sent_count = 0

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

_VALID_TYPES = {"open", "close", "reduce", "adjust_tp", "adjust_sl"}

def _validate_env(env: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Validate envelope-level schema.
    """
    if not isinstance(env, dict):
        return None

    # Basic fields
    if "action" not in env or not isinstance(env["action"], dict):
        return None

    label = env.get("label")
    if not isinstance(label, str):
        return None

    # Only process actions for THIS ACCOUNT_LABEL
    if label != ACCOUNT_LABEL:
        return None

    # Timestamp required for ordering
    ts = env.get("ts_ms")
    if not isinstance(ts, int):
        return None

    return env


def _validate_action(act: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Validate an AI action dict.
    """
    t = str(act.get("type", "")).lower()
    if t not in _VALID_TYPES:
        return None

    symbol = act.get("symbol")
    if not isinstance(symbol, str) or not symbol:
        return None

    out = {
        "type": t,
        "symbol": symbol,
        "side": act.get("side"),
        "qty": act.get("qty"),
        "reason": act.get("reason", "unspecified"),
        "extra": act.get("extra") if isinstance(act.get("extra"), dict) else {},
    }

    # Sanitize side
    if out["side"]:
        s = str(out["side"]).lower()
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

    return out


# ---------------------------------------------------------------------------
# File tailing
# ---------------------------------------------------------------------------

def _iter_new_envelopes(path: Path, offset: int) -> Tuple[int, List[Dict[str, Any]]]:
    """
    Safe tailing with truncation detection.
    """
    if not path.exists():
        return offset, []

    try:
        size = path.stat().st_size
    except Exception:
        return offset, []

    # File truncated (log rotation, cleanup, etc.)
    if offset > size:
        logger.warning("ai_action_router: file truncated; resetting offset to 0")
        offset = 0

    envs: List[Dict[str, Any]] = []
    try:
        with path.open("rb") as f:
            f.seek(offset)
            for line in f:
                offset += len(line)
                line = line.strip()
                if not line:
                    continue
                try:
                    env = orjson.loads(line)
                    if isinstance(env, dict):
                        envs.append(env)
                except Exception:
                    continue
    except Exception as e:
        alert_bot_error("ai_action_router", f"read error: {e}", "ERROR")
        return offset, []

    return offset, envs


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def _fmt_env_to_text(env: Dict[str, Any], act: Dict[str, Any]) -> str:
    """
    Safe, readable formatting for Telegram.
    """
    lines = []
    lines.append("🤖 AI Action (DRY-RUN)")
    lines.append(f"• label  : {env.get('label')}")
    lines.append(f"• type   : {act['type']}")
    lines.append(f"• symbol : {act['symbol']}")
    lines.append(f"• side   : {act.get('side')}")
    lines.append(f"• qty    : {act.get('qty')}")
    lines.append(f"• reason : {act.get('reason')}")

    extra = act.get("extra") or {}
    if extra:
        kv = ", ".join(f"{k}={extra[k]}" for k in list(extra.keys())[:5])
        lines.append(f"• extra  : {kv}")

    return "\n".join(lines)


def _tg_throttle() -> bool:
    """
    Returns True if allowed to send, False if throttled.
    """
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
        "AI Action Router starting (label=%s, poll=%s, tg=%s)",
        ACCOUNT_LABEL, POLL_SECONDS, SEND_TG
    )

    # Start at EOF to avoid spam
    try:
        offset = ACTION_LOG_PATH.stat().st_size if ACTION_LOG_PATH.exists() else 0
    except Exception:
        offset = 0

    # Startup notice
    try:
        send_tg(f"📡 AI Action Router online for {ACCOUNT_LABEL}")
    except Exception:
        pass

    while True:
        record_heartbeat("ai_action_router")

        try:
            offset, envs = _iter_new_envelopes(ACTION_LOG_PATH, offset)

            for env in envs:
                venv = _validate_env(env)
                if venv is None:
                    continue

                vact = _validate_action(venv["action"])
                if vact is None:
                    continue

                text = _fmt_env_to_text(venv, vact)
                logger.info(text)

                if SEND_TG and _tg_throttle():
                    try:
                        send_tg(text)
                    except Exception as e:
                        alert_bot_error("ai_action_router", f"tg error: {e}", "WARN")

        except Exception as e:
            alert_bot_error("ai_action_router", f"loop error: {e}", "ERROR")

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    loop()
