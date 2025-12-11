#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Action Router v0.3 (guarded + ExecSignal queue)

Purpose
-------
Guarded AI Action Router with ExecSignal queue.

- Watches the JSONL actions file produced by ai_pilot for a single ACCOUNT_LABEL.
- For each new action:
    • Runs it through the AI Action Guard (schema + whitelist + risk sanity).
    • Classifies it as:
        - heartbeat/noop
        - rejected trade-like (invalid)
        - accepted trade-like (valid)
    • For accepted trade-like actions:
        - Builds a normalized ExecSignal (DRY-RUN by default).
        - Appends it to EXEC_SIGNALS_PATH (JSONL queue).
- Does NOT place orders. Executor is a separate process.

Supervisor expects this module as: app.tools.ai_action_router
with a main() and normal __main__ entrypoint.
"""

from __future__ import annotations

import os
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Tuple

import orjson

# ---------------------------------------------------------------------------
# Logger (robust import)
# ---------------------------------------------------------------------------

try:
    import importlib

    _log_module = importlib.import_module("app.core.log")
    get_logger = getattr(_log_module, "get_logger")
except Exception:
    import logging

    def get_logger(name: str) -> "logging.Logger":  # type: ignore
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


logger = get_logger("ai_action_router")

# ---------------------------------------------------------------------------
# Env / paths
# ---------------------------------------------------------------------------

ACCOUNT_LABEL: str = os.getenv("ACCOUNT_LABEL", "main").strip() or "main"
POLL_SECONDS: int = int(os.getenv("AI_ACTION_ROUTER_POLL_SECONDS", "2"))

# Prefer a single AI_ACTIONS_PATH (aligned with ai_pilot + tools).
# Fallbacks:
#   - settings.AI_ACTIONS_PATH
#   - "state/ai_actions.jsonl"
try:
    from app.core.config import settings  # type: ignore

    default_actions_path = getattr(settings, "AI_ACTIONS_PATH", "state/ai_actions.jsonl")
    default_exec_path = getattr(settings, "EXEC_SIGNALS_PATH", "state/exec_signals.jsonl")
except Exception:
    default_actions_path = "state/ai_actions.jsonl"
    default_exec_path = "state/exec_signals.jsonl"

env_actions_path = os.getenv("AI_ACTIONS_PATH", "").strip()
_actions_path_str = env_actions_path or default_actions_path

env_exec_path = os.getenv("EXEC_SIGNALS_PATH", "").strip()
_exec_path_str = env_exec_path or default_exec_path

ACTIONS_FILE: Path = Path(_actions_path_str).resolve()
ACTIONS_FILE.parent.mkdir(parents=True, exist_ok=True)

EXEC_SIGNALS_PATH: Path = Path(_exec_path_str).resolve()
EXEC_SIGNALS_PATH.parent.mkdir(parents=True, exist_ok=True)

logger.info(
    "AI Action Router configured for ACCOUNT_LABEL=%s, ACTIONS_FILE=%s, EXEC_SIGNALS_PATH=%s, poll=%ss",
    ACCOUNT_LABEL,
    ACTIONS_FILE,
    EXEC_SIGNALS_PATH,
    POLL_SECONDS,
)

# ---------------------------------------------------------------------------
# Guard import
# ---------------------------------------------------------------------------

from app.core.ai_action_guard import (
    load_guard_config,
    guard_action,
)

from app.core.exec_signal_schema import (
    ExecSignal,
    missing_exec_fields,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_new_actions(last_size: int) -> Tuple[List[Dict[str, Any]], int]:
    """
    Very simple "tail" reader on the JSONL actions file.

    - last_size is the previous file size in bytes.
    - Returns (new_actions, new_file_size).
    """
    if not ACTIONS_FILE.exists():
        return [], last_size

    try:
        raw = ACTIONS_FILE.read_bytes()
    except Exception as exc:
        logger.error("Failed to read actions file %s: %s", ACTIONS_FILE, exc)
        return [], last_size

    new_size = len(raw)

    if new_size <= last_size:
        # No new data
        return [], new_size

    chunk = raw[last_size:]
    lines = chunk.splitlines()

    actions: List[Dict[str, Any]] = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            obj = orjson.loads(line)
        except Exception:
            logger.debug("Skipping malformed action line: %r", line)
            continue
        if isinstance(obj, dict):
            actions.append(obj)

    return actions, new_size


def _append_exec_signal(exec_sig: ExecSignal) -> None:
    """
    Append one ExecSignal to EXEC_SIGNALS_PATH as JSONL.

    This is the only place the router writes to the exec queue.
    """
    try:
        payload = orjson.dumps(exec_sig)
        with EXEC_SIGNALS_PATH.open("ab") as f:
            f.write(payload)
            f.write(b"\n")
    except Exception as exc:
        logger.error("Failed to append ExecSignal to %s: %s", EXEC_SIGNALS_PATH, exc)


def _map_ai_action_to_exec_signal(
    action: Dict[str, Any],
) -> ExecSignal:
    """
    Map a VALIDATED AIAction (trade-like) into a normalized ExecSignal.

    Notes:
    - This function assumes guard_action(...) has already validated the
      structure and basic risk fields. It still double-checks core
      exec requirements for safety.
    - Sizing is deliberately conservative (qty=0.0 for now) until you
      decide how to translate risk_R -> position size.
    """
    ts_ms = int(action.get("ts_ms") or action.get("timestamp_ms") or int(time.time() * 1000))
    account_label = str(action.get("account_label") or ACCOUNT_LABEL)

    # AI side/type -> exec side/action mapping
    ai_type = str(action.get("type") or "").lower()
    ai_side = str(action.get("side") or "").lower()
    symbol = str(action.get("symbol") or "").upper()

    # Default mappings; refine later when ai_pilot schema is enforced.
    if ai_side == "long":
        side = "buy"
    elif ai_side == "short":
        side = "sell"
    else:
        # Fallback: treat unknown as buy, but this should be rare once schema is enforced.
        side = "buy"

    if ai_type in ("open", "entry"):
        action_kind = "open"
    elif ai_type in ("add", "scale_in"):
        action_kind = "add"
    elif ai_type in ("reduce", "scale_out"):
        action_kind = "reduce"
    elif ai_type in ("close", "exit"):
        action_kind = "close"
    elif ai_type in ("close_all", "flatten"):
        action_kind = "close_all_account"
    else:
        # Fallback: treat unknown type as "open" until ai_pilot is upgraded.
        action_kind = "open"

    # DRY_RUN enforcement: we NEVER send live orders from router v0.3.
    dry_run_flag = True

    # For now, we keep qty=0.0. When you implement proper sizing logic,
    # this will be replaced by a risk-aware size based on risk_R, ATR, etc.
    qty = 0.0

    # Order type: start conservative and simple.
    order_type = "market"
    tif = "IOC"  # immediate-or-cancel by default in this scaffold phase.

    entry_hint = action.get("entry_hint")
    sl_hint = action.get("sl_hint")
    tp_hint = action.get("tp_hint")

    exec_sig: ExecSignal = {
        "ts_ms": ts_ms,
        "account_label": account_label,
        "signal_id": f"exec_{uuid.uuid4().hex}",
        "source": "ai_action_router",

        "symbol": symbol,
        "side": side,              # buy / sell
        "action": action_kind,     # open/add/reduce/close/...

        "qty": float(qty),
        "order_type": order_type,
        "time_in_force": tif,
        "price": entry_hint,
        "sl_price": sl_hint,
        "tp_price": tp_hint,

        "ai_action_id": str(action.get("action_id") or ""),
        "strategy_role": str(action.get("strategy_role") or ""),
        "tags": list(action.get("tags") or []),

        "dry_run": dry_run_flag,
        "reduce_only": None,
        "post_only": None,

        "extra": {
            "from_ai_action": action,
        },
    }

    # Final safety: ensure we didn't miss required exec fields.
    missing = missing_exec_fields(exec_sig)
    if missing:
        # This SHOULD NOT happen if ai_action is sane. If it does, log loudly.
        logger.warning(
            "ExecSignal missing required fields %s for account=%s symbol=%s action=%s",
            sorted(missing.keys()),
            account_label,
            symbol,
            action_kind,
        )

    return exec_sig


# ---------------------------------------------------------------------------
# Core loop
# ---------------------------------------------------------------------------

def main() -> None:
    logger.info(
        "AI Action Router starting for ACCOUNT_LABEL=%s, actions_file=%s, exec_queue=%s, poll=%ss",
        ACCOUNT_LABEL,
        ACTIONS_FILE,
        EXEC_SIGNALS_PATH,
        POLL_SECONDS,
    )

    cfg = load_guard_config()
    logger.info(
        "AI Action Guard config: allowed_symbols=%s, require_whitelist=%s, max_notional_pct=%.2f",
        cfg.allowed_symbols,
        cfg.require_whitelist,
        cfg.max_notional_pct,
    )

    last_size = 0

    while True:
        try:
            actions, last_size = _load_new_actions(last_size)
            if actions:
                logger.info(
                    "AI Action Router saw %d new actions for %s",
                    len(actions),
                    ACCOUNT_LABEL,
                )

                for raw_action in actions:
                    # 1) Run through guard
                    res = guard_action(raw_action, cfg)

                    # 2) Heartbeats / noop / empty
                    if res.is_heartbeat:
                        if res.ok:
                            logger.info(
                                "AI heartbeat action accepted for %s: %s",
                                ACCOUNT_LABEL,
                                res.action,
                            )
                        else:
                            logger.warning(
                                "AI heartbeat action REJECTED for %s: reasons=%s action=%s",
                                ACCOUNT_LABEL,
                                res.reasons,
                                res.action,
                            )
                        continue

                    # 3) Trade-like actions
                    if not res.ok:
                        logger.warning(
                            "AI trade-like action REJECTED for %s: reasons=%s action=%s",
                            ACCOUNT_LABEL,
                            res.reasons,
                            res.action,
                        )
                        continue

                    # At this point, we have a structurally valid trade-like action.
                    a = res.action
                    logger.info(
                        "AI trade-like action ACCEPTED for %s: type=%s symbol=%s side=%s risk_R=%s expected_R=%s",
                        ACCOUNT_LABEL,
                        a.get("type"),
                        a.get("symbol"),
                        a.get("side"),
                        a.get("risk_R"),
                        a.get("expected_R"),
                    )

                    # 4) Map to ExecSignal (DRY-RUN) and append to queue.
                    exec_sig = _map_ai_action_to_exec_signal(a)
                    _append_exec_signal(exec_sig)
                    logger.info(
                        "ExecSignal appended for %s: symbol=%s side=%s action=%s dry_run=%s",
                        ACCOUNT_LABEL,
                        exec_sig.get("symbol"),
                        exec_sig.get("side"),
                        exec_sig.get("action"),
                        exec_sig.get("dry_run"),
                    )

            time.sleep(POLL_SECONDS)

        except KeyboardInterrupt:
            logger.info("AI Action Router interrupted by user, exiting.")
            break
        except Exception as e:
            logger.exception("AI Action Router error: %s", e)
            time.sleep(3)


if __name__ == "__main__":
    main()
