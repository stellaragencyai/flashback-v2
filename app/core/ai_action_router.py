#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Action Router (Canonical)

IMPORTANT
---------
This module is the ONE canonical implementation for AI action routing.

It provides:
1) Execution router (WS-first): normalize_ai_action / apply_ai_action(s)
2) DRY-RUN tailer router: tails action bus (state/ai_actions.jsonl) and logs/TG
3) ExecSignal queue router: guarded router that maps validated AI actions into ExecSignal queue

Other modules (app/bots/ai_action_router.py and app/tools/ai_action_router.py)
MUST be thin adapters that call into this canonical implementation.
"""

from __future__ import annotations

import os
import time
import uuid
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import orjson

# ---------------------------------------------------------------------------
# Logger (robust import)
# ---------------------------------------------------------------------------

try:
    from app.core.log import get_logger  # type: ignore
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

# ---------------------------------------------------------------------------
# Core helpers
# ---------------------------------------------------------------------------

from app.core.flashback_common import (
    send_tg,
    alert_bot_error,
    record_heartbeat,
)

# Execution WS-first functions (LIVE side)
from app.core.execution_ws import (
    open_position_ws_first,
    flatten_symbol_ws_first,
    list_open_symbols,
)

from app.core.ai_profile import get_current_ai_profile

# Action bus path (DRY-RUN tailer)
from app.core.ai_action_bus import ACTION_LOG_PATH

# Guard + exec signal schema (QUEUE router)
from app.core.ai_action_guard import (
    load_guard_config,
    guard_action,
)

from app.core.exec_signal_schema import (
    ExecSignal,
    missing_exec_fields,
)

# ---------------------------------------------------------------------------
# Env helpers
# ---------------------------------------------------------------------------

def _env_bool(name: str, default: str = "false") -> bool:
    raw = os.getenv(name, default).strip().lower()
    return raw in ("1", "true", "yes", "y", "on")


def _env_int(name: str, default: str) -> int:
    try:
        return int(os.getenv(name, default).strip())
    except Exception:
        return int(default)


# ---------------------------------------------------------------------------
# (1) Execution Router — Strict normalized contract (OPEN/FLATTEN/FLATTEN_ALL/NOP)
# ---------------------------------------------------------------------------

def _to_decimal(val: Any, field: str) -> Decimal:
    try:
        return Decimal(str(val))
    except Exception:
        raise ValueError(f"Invalid decimal for {field}: {val!r}")


def _normalize_symbol(sym: Any) -> str:
    s = str(sym or "").strip().upper()
    if not s:
        raise ValueError("symbol is required and cannot be empty")
    return s


def _normalize_side(side: Any) -> str:
    s = str(side or "").strip().upper()
    if s not in ("LONG", "SHORT"):
        raise ValueError("side must be 'LONG' or 'SHORT'")
    return s


def _validate_symbol_whitelist(symbol: str, profile: Dict[str, Any]) -> None:
    require = bool(profile.get("require_whitelist", False))
    allowed = profile.get("allowed_symbols") or []
    if not require:
        return
    if allowed and symbol not in allowed:
        raise ValueError(
            f"symbol '{symbol}' is not in AI allowed_symbols for account "
            f"{profile.get('account_label')}"
        )


def _validate_notional_pct(pct: Decimal, profile: Dict[str, Any]) -> None:
    if pct <= 0:
        raise ValueError("risk_pct_notional must be > 0")
    max_pct = profile.get("max_notional_pct")
    if isinstance(max_pct, Decimal) and max_pct > 0 and pct > max_pct:
        raise ValueError(
            f"risk_pct_notional {pct}% exceeds AI profile max_notional_pct={max_pct}%"
        )


def _normalize_open_action(payload: Dict[str, Any], profile: Dict[str, Any]) -> Dict[str, Any]:
    symbol = _normalize_symbol(payload.get("symbol"))

    # Side: accept LONG/SHORT, also accept buy/sell/long/short
    raw_side = payload.get("side")
    side_s = str(raw_side or "").strip().lower()
    if side_s in ("buy", "long"):
        side = "LONG"
    elif side_s in ("sell", "short"):
        side = "SHORT"
    else:
        side = _normalize_side(raw_side)

    # risk_pct_notional may be missing or explicitly null in JSONL
    rpn_raw = payload.get("risk_pct_notional", "__MISSING__")
    if rpn_raw in ("__MISSING__", None):
        # Compute from size_fraction * profile.max_notional_pct if possible
        max_pct = profile.get("max_notional_pct")
        if not isinstance(max_pct, Decimal):
            try:
                max_pct = Decimal(str(max_pct))
            except Exception:
                max_pct = Decimal("40")  # safe fallback

        sf_raw = payload.get("size_fraction", None)
        if sf_raw is not None:
            try:
                sf = Decimal(str(sf_raw))
            except Exception:
                sf = Decimal("0")
            if sf <= 0:
                raise ValueError("size_fraction must be > 0 when used")
            if sf > 1:
                sf = Decimal("1")
            risk_pct_notional = max_pct * sf
        else:
            # Conservative fallback: min(max_pct, 1%)
            risk_pct_notional = max_pct if max_pct < Decimal("1.0") else Decimal("1.0")
    else:
        risk_pct_notional = _to_decimal(rpn_raw, "risk_pct_notional")

    _validate_symbol_whitelist(symbol, profile)
    _validate_notional_pct(risk_pct_notional, profile)

    # Optional spread cap
    max_spread_raw = payload.get("max_spread_bps", None)
    if max_spread_raw is None:
        max_spread_bps = None
    else:
        try:
            max_spread_bps = _to_decimal(max_spread_raw, "max_spread_bps")
            if max_spread_bps <= 0:
                max_spread_bps = None
        except Exception:
            max_spread_bps = None

    # Optional leverage override
    lev_raw = payload.get("leverage_override", None)
    if lev_raw is None:
        lev = None
    else:
        try:
            lev_int = int(lev_raw)
            lev = lev_int if lev_int > 0 else None
        except Exception:
            raise ValueError(f"Invalid leverage_override: {lev_raw!r}")

    return {
        "type": "OPEN",
        "symbol": symbol,
        "side": side,
        "risk_pct_notional": risk_pct_notional,
        "max_spread_bps": max_spread_bps,
        "leverage_override": lev,
    }

def _normalize_flatten_action(payload: Dict[str, Any], profile: Dict[str, Any]) -> Dict[str, Any]:
    symbol = _normalize_symbol(payload.get("symbol"))
    _validate_symbol_whitelist(symbol, profile)
    return {"type": "FLATTEN", "symbol": symbol}


def _normalize_flatten_all(_: Dict[str, Any]) -> Dict[str, Any]:
    return {"type": "FLATTEN_ALL"}


def _normalize_nop(_: Dict[str, Any]) -> Dict[str, Any]:
    return {"type": "NOP"}


def normalize_ai_action(raw_action: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate + normalize a raw AI action dict, using current AI profile.
    Returns canonical dict or raises ValueError.
    """
    if not isinstance(raw_action, dict):
        raise ValueError("AI action must be a dict")


    # Sanitize explicit nulls so fallback sizing logic can run
    if raw_action.get("risk_pct_notional", "__MISSING__") is None:
        raw_action = dict(raw_action)
        raw_action.pop("risk_pct_notional", None)

    if raw_action.get("max_spread_bps", "__MISSING__") is None:
        raw_action = dict(raw_action)
        raw_action.pop("max_spread_bps", None)

    if raw_action.get("leverage_override", "__MISSING__") is None:
        raw_action = dict(raw_action)
        raw_action.pop("leverage_override", None)
    profile = get_current_ai_profile()

    action_type = str(raw_action.get("type") or "").strip().upper()
    if not action_type:
        raise ValueError("AI action missing 'type' field")

    payload = dict(raw_action)

    if action_type == "OPEN":
        return _normalize_open_action(payload, profile)
    if action_type == "FLATTEN":
        return _normalize_flatten_action(payload, profile)
    if action_type == "FLATTEN_ALL":
        return _normalize_flatten_all(payload)
    if action_type == "NOP":
        return _normalize_nop(payload)

    raise ValueError(f"Unsupported AI action type: {action_type!r}")


def apply_ai_action(raw_action: Dict[str, Any]) -> Dict[str, Any]:
    """
    Main entry point: validate, normalize, and apply a SINGLE AI action (LIVE via WS-first).
    """
    record_heartbeat("ai_action_router")

    try:
        normalized = normalize_ai_action(raw_action)
    except Exception as e:
        msg = f"AI action validation failed: {e}"
        alert_bot_error("ai_action_router", msg, "WARN")
        return {"ok": False, "error": msg, "normalized": None, "result": None}

    a_type = normalized["type"]

    if a_type == "NOP":
        return {"ok": True, "error": None, "normalized": normalized, "result": {"noop": True}}

    try:
        if a_type == "OPEN":
            res = open_position_ws_first(
                symbol=normalized["symbol"],
                side=normalized["side"],
                risk_pct_notional=normalized["risk_pct_notional"],
                max_spread_bps=normalized["max_spread_bps"],
                leverage_override=normalized["leverage_override"],
                notify=True,
            )
            return {"ok": True, "error": None, "normalized": normalized, "result": res}

        if a_type == "FLATTEN":
            res = flatten_symbol_ws_first(symbol=normalized["symbol"], notify=True)
            return {"ok": True, "error": None, "normalized": normalized, "result": res}

        if a_type == "FLATTEN_ALL":
            results: Dict[str, Any] = {}
            symbols = list_open_symbols()
            for sym in symbols:
                try:
                    r = flatten_symbol_ws_first(sym, notify=True)
                    results[sym] = {"ok": True, "result": r}
                except Exception as e_flat:
                    err_msg = f"Flatten {sym} failed: {e_flat}"
                    alert_bot_error("ai_action_router", err_msg, "ERROR")
                    results[sym] = {"ok": False, "error": str(e_flat)}
            return {"ok": True, "error": None, "normalized": normalized, "result": results}

        raise RuntimeError(f"Unknown normalized action type {a_type!r}")

    except Exception as e:
        msg = f"AI action execution error: {e}"
        alert_bot_error("ai_action_router", msg, "ERROR")
        try:
            send_tg(f"AI action failed: {msg}")
        except Exception:
            pass
        return {"ok": False, "error": msg, "normalized": normalized, "result": None}


def apply_ai_actions(raw_actions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Apply a LIST of AI actions sequentially.
    """
    if not isinstance(raw_actions, list):
        raise ValueError("AI actions must be a list of dicts")

    out: List[Dict[str, Any]] = []
    for a in raw_actions:
        try:
            res = apply_ai_action(a)
        except Exception as e:
            msg = f"Unexpected error applying action {a}: {e}"
            alert_bot_error("ai_action_router", msg, "ERROR")
            out.append({"ok": False, "error": msg, "normalized": None, "result": None})
        else:
            out.append(res)
    return out


# ---------------------------------------------------------------------------
# (2) DRY-RUN Router — Tails ACTION_LOG_PATH and logs/TG (no live orders)
# ---------------------------------------------------------------------------

_VALID_TYPES_DRY = {"open", "close", "reduce", "adjust_tp", "adjust_sl"}

@dataclass
class DryRunRouterConfig:
    account_label: str
    enabled: bool
    poll_seconds: int
    send_tg: bool
    max_tg_per_minute: int


def _validate_env_dry(env: Dict[str, Any], account_label: str) -> Optional[Dict[str, Any]]:
    if not isinstance(env, dict):
        return None
    if "action" not in env or not isinstance(env["action"], dict):
        return None
    label = env.get("label")
    if not isinstance(label, str):
        return None
    if label != account_label:
        return None
    ts = env.get("ts_ms")
    if not isinstance(ts, int):
        return None
    return env


def _validate_action_dry(act: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    t = str(act.get("type", "")).lower()
    if t not in _VALID_TYPES_DRY:
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

    if out["side"]:
        s = str(out["side"]).lower()
        if s not in ("buy", "sell", "long", "short"):
            return None
        out["side"] = s

    if out["qty"] is not None:
        try:
            q = float(out["qty"])
            if q < 0:
                return None
            out["qty"] = q
        except Exception:
            return None

    return out


def _iter_new_envelopes(path: Path, offset: int) -> Tuple[int, List[Dict[str, Any]]]:
    if not path.exists():
        return offset, []

    try:
        size = path.stat().st_size
    except Exception:
        return offset, []

    if offset > size:
        logger.warning("ai_action_router(dry): file truncated; resetting offset to 0")
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
        alert_bot_error("ai_action_router", f"dry read error: {e}", "ERROR")
        return offset, []

    return offset, envs


def _fmt_env_to_text(env: Dict[str, Any], act: Dict[str, Any]) -> str:
    lines = []
    lines.append("AI Action (DRY-RUN)")
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


def _tg_throttle_state(max_per_minute: int, sent_count: int, last_minute: int) -> Tuple[bool, int, int]:
    now_min = int(time.time() // 60)
    if now_min != last_minute:
        last_minute = now_min
        sent_count = 0
    if sent_count >= max_per_minute:
        return False, sent_count, last_minute
    sent_count += 1
    return True, sent_count, last_minute


def dry_run_router_loop() -> None:
    """
    Canonical DRY-RUN router loop (what app/bots/ai_action_router.py should run).
    """
    cfg = DryRunRouterConfig(
        account_label=os.getenv("ACCOUNT_LABEL", "main").strip() or "main",
        enabled=_env_bool("AI_ROUTER_ENABLED", "true"),
        poll_seconds=_env_int("AI_ROUTER_POLL_SECONDS", "2"),
        send_tg=_env_bool("AI_ROUTER_SEND_TG", "true"),
        max_tg_per_minute=60,
    )

    if not cfg.enabled:
        logger.warning("AI Action Router (DRY-RUN) disabled. Exiting.")
        return

    logger.info(
        "AI Action Router (DRY-RUN) starting (label=%s, poll=%ss, tg=%s)",
        cfg.account_label, cfg.poll_seconds, cfg.send_tg
    )

    try:
        offset = ACTION_LOG_PATH.stat().st_size if ACTION_LOG_PATH.exists() else 0
    except Exception:
        offset = 0

    try:
        send_tg(f"AI Action Router online for {cfg.account_label} (DRY-RUN)")
    except Exception:
        pass

    sent_count = 0
    last_minute = int(time.time() // 60)

    while True:
        record_heartbeat("ai_action_router")

        try:
            offset, envs = _iter_new_envelopes(ACTION_LOG_PATH, offset)

            for env in envs:
                venv = _validate_env_dry(env, cfg.account_label)
                if venv is None:
                    continue

                vact = _validate_action_dry(venv["action"])
                if vact is None:
                    continue

                text = _fmt_env_to_text(venv, vact)
                logger.info(text)

                if cfg.send_tg:
                    allowed, sent_count, last_minute = _tg_throttle_state(cfg.max_tg_per_minute, sent_count, last_minute)
                    if allowed:
                        try:
                            send_tg(text)
                        except Exception as e:
                            alert_bot_error("ai_action_router", f"dry tg error: {e}", "WARN")

        except Exception as e:
            alert_bot_error("ai_action_router", f"dry loop error: {e}", "ERROR")

        time.sleep(cfg.poll_seconds)


# ---------------------------------------------------------------------------
# (3) ExecSignal Queue Router — Guarded router that appends ExecSignal JSONL queue
# ---------------------------------------------------------------------------

def _resolve_paths_for_queue_router() -> Tuple[Path, Path]:
    """
    Resolve actions + exec queue paths:
      - prefer env (AI_ACTIONS_PATH, EXEC_SIGNALS_PATH)
      - fallback to settings if available
      - fallback to state/*.jsonl
    """
    ACCOUNT_LABEL = os.getenv("ACCOUNT_LABEL", "main").strip() or "main"

    try:
        from app.core.config import settings  # type: ignore
        default_actions_path = getattr(settings, "AI_ACTIONS_PATH", "state/ai_actions.jsonl")
        default_exec_path = getattr(settings, "EXEC_SIGNALS_PATH", "state/exec_signals.jsonl")
    except Exception:
        default_actions_path = "state/ai_actions.jsonl"
        default_exec_path = "state/exec_signals.jsonl"

    env_actions_path = os.getenv("AI_ACTIONS_PATH", "").strip()
    env_exec_path = os.getenv("EXEC_SIGNALS_PATH", "").strip()

    actions_path = Path(env_actions_path or default_actions_path).resolve()
    exec_path = Path(env_exec_path or default_exec_path).resolve()

    actions_path.parent.mkdir(parents=True, exist_ok=True)
    exec_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info(
        "Queue Router configured for ACCOUNT_LABEL=%s, ACTIONS_FILE=%s, EXEC_SIGNALS_PATH=%s",
        ACCOUNT_LABEL, actions_path, exec_path
    )

    return actions_path, exec_path


def _load_new_actions_bytes(actions_file: Path, last_size: int) -> Tuple[List[Dict[str, Any]], int]:
    if not actions_file.exists():
        return [], last_size

    try:
        raw = actions_file.read_bytes()
    except Exception as exc:
        logger.error("Failed to read actions file %s: %s", actions_file, exc)
        return [], last_size

    new_size = len(raw)
    if new_size <= last_size:
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
            logger.debug("Skipping malformed action line.")
            continue
        if isinstance(obj, dict):
            actions.append(obj)

    return actions, new_size


def _append_exec_signal(exec_signals_path: Path, exec_sig: ExecSignal) -> None:
    try:
        payload = orjson.dumps(exec_sig)
        with exec_signals_path.open("ab") as f:
            f.write(payload)
            f.write(b"\n")
    except Exception as exc:
        logger.error("Failed to append ExecSignal to %s: %s", exec_signals_path, exc)


def _map_ai_action_to_exec_signal(action: Dict[str, Any], account_label_fallback: str) -> ExecSignal:
    ts_ms = int(action.get("ts_ms") or action.get("timestamp_ms") or int(time.time() * 1000))
    account_label = str(action.get("account_label") or account_label_fallback)

    ai_type = str(action.get("type") or "").lower()
    ai_side = str(action.get("side") or "").lower()
    symbol = str(action.get("symbol") or "").upper()

    if ai_side == "long":
        side = "buy"
    elif ai_side == "short":
        side = "sell"
    else:
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
        action_kind = "open"

    dry_run_flag = True
    qty = 0.0

    order_type = "market"
    tif = "IOC"

    entry_hint = action.get("entry_hint")
    sl_hint = action.get("sl_hint")
    tp_hint = action.get("tp_hint")

    exec_sig: ExecSignal = {
        "ts_ms": ts_ms,
        "account_label": account_label,
        "signal_id": f"exec_{uuid.uuid4().hex}",
        "source": "ai_action_router",

        "symbol": symbol,
        "side": side,
        "action": action_kind,

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

        "extra": {"from_ai_action": action},
    }

    missing = missing_exec_fields(exec_sig)
    if missing:
        logger.warning(
            "ExecSignal missing required fields %s for account=%s symbol=%s action=%s",
            sorted(missing.keys()),
            account_label,
            symbol,
            action_kind,
        )

    return exec_sig


def execsignal_queue_router_main() -> None:
    """
    Canonical queue router main (what app/tools/ai_action_router.py should run).
    """
    account_label = os.getenv("ACCOUNT_LABEL", "main").strip() or "main"
    poll_seconds = int(os.getenv("AI_ACTION_ROUTER_POLL_SECONDS", "2"))

    actions_file, exec_signals_path = _resolve_paths_for_queue_router()

    logger.info(
        "Queue Router starting for ACCOUNT_LABEL=%s, actions_file=%s, exec_queue=%s, poll=%ss",
        account_label,
        actions_file,
        exec_signals_path,
        poll_seconds,
    )

    cfg = load_guard_config()
    logger.info(
        "AI Action Guard config: allowed_symbols=%s require_whitelist=%s max_notional_pct=%.2f",
        cfg.allowed_symbols,
        cfg.require_whitelist,
        cfg.max_notional_pct,
    )

    last_size = 0

    while True:
        try:
            actions, last_size = _load_new_actions_bytes(actions_file, last_size)
            if actions:
                logger.info("Queue Router saw %d new actions for %s", len(actions), account_label)

                for raw_action in actions:
                    res = guard_action(raw_action, cfg)

                    if res.is_heartbeat:
                        if res.ok:
                            logger.info("AI heartbeat accepted for %s: %s", account_label, res.action)
                        else:
                            logger.warning("AI heartbeat REJECTED for %s reasons=%s action=%s", account_label, res.reasons, res.action)
                        continue

                    if not res.ok:
                        logger.warning("AI action REJECTED for %s reasons=%s action=%s", account_label, res.reasons, res.action)
                        continue

                    a = res.action
                    logger.info(
                        "AI action ACCEPTED for %s type=%s symbol=%s side=%s risk_R=%s expected_R=%s",
                        account_label,
                        a.get("type"),
                        a.get("symbol"),
                        a.get("side"),
                        a.get("risk_R"),
                        a.get("expected_R"),
                    )

                    exec_sig = _map_ai_action_to_exec_signal(a, account_label_fallback=account_label)
                    _append_exec_signal(exec_signals_path, exec_sig)

                    logger.info(
                        "ExecSignal appended for %s symbol=%s side=%s action=%s dry_run=%s",
                        account_label,
                        exec_sig.get("symbol"),
                        exec_sig.get("side"),
                        exec_sig.get("action"),
                        exec_sig.get("dry_run"),
                    )

            time.sleep(poll_seconds)

        except KeyboardInterrupt:
            logger.info("Queue Router interrupted by user, exiting.")
            break
        except Exception as e:
            logger.exception("Queue Router error: %s", e)
            time.sleep(3)
