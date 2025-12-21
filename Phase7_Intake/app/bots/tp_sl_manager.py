#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — TP/SL Manager v6.16 (PAPER ledger overlay + trade_id-linked TP orderLinkIds + Phase4 decision gate hardened + CONSOLE PROOF)

v6.15 Patch (PAPER positions visibility):
- tp_sl_manager can now "see" PAPER positions opened by PaperBroker by reading:
    state/paper/<ACCOUNT_LABEL>.json
- Converts paper open_positions into position dicts compatible with _ensure_exits_for_position()
- Merges PAPER positions into the positions list returned by position_bus/REST

v6.16 Patch (Phase 4 Step 3 proof):
- When AI gate blocks TP/SL actions, we print a deterministic CONSOLE line:
    [tp_sl_manager] 🚫 GATE_BLOCKED symbol=... trade_id=... reason=...
  This is in addition to alert_bot_error() + Telegram.
"""

import os
import time
import json
from decimal import Decimal
from pathlib import Path
from typing import Dict, Tuple, List, Optional, Any

import yaml  # type: ignore

from app.core.flashback_common import (
    bybit_get,
    bybit_post,
    send_tg,
    get_ticks,
    psnap,
    qdown,
    last_price,
    atr14,
    set_stop_loss,
    cancel_all,      # kept for emergencies only; not used in normal flow  # noqa: F401
    place_reduce_tp,
    BYBIT_WS_PRIVATE_URL,
    build_ws_auth_payload_main,
    record_heartbeat,
    alert_bot_error,
)

from app.core.position_bus import get_positions_snapshot as bus_get_positions_snapshot

# -------------------------
# Phase 4: AI decision enforcement (optional)
# -------------------------
try:
    from app.ai.ai_decision_enforcer import enforce_decision  # type: ignore
except Exception:
    enforce_decision = None  # type: ignore

# Throttle gate block noise (symbol -> last_notice_ts)
_GATE_BLOCK_THROTTLE_SEC = float(os.getenv("TPM_GATE_BLOCK_THROTTLE_SEC", "30"))
_GATE_BLOCK_LAST: Dict[str, float] = {}

# Deterministic PAPER test override (useful for Step 3 proof)
_TPM_FORCE_TRADE_ID = os.getenv("TPM_FORCE_TRADE_ID", "").strip()

try:
    import websocket  # type: ignore
except ImportError:
    websocket = None

try:
    from app.core import strategies as strat_mod
except Exception:
    strat_mod = None  # type: ignore

try:
    from app.core.config import settings
except Exception:
    settings = None  # type: ignore


# ---- Spacing params from common module if present ----
try:
    from app.core.flashback_common import ATR_MULT, TP5_MAX_ATR_MULT, TP5_MAX_PCT, R_MIN_TICKS
except Exception:
    ATR_MULT = Decimal("1.0")
    TP5_MAX_ATR_MULT = Decimal("3.0")
    TP5_MAX_PCT = Decimal("6.0")
    R_MIN_TICKS = 3


CATEGORY = "linear"
QUOTE = "USDT"

CORE_TP_COUNT = 10
POLL_SECONDS = int(os.getenv("TPM_POLL_SECONDS", "2"))
USE_WS = os.getenv("TPM_USE_WEBSOCKET", "false").strip().lower() == "true"
_RESPECT_MANUAL_TPS = os.getenv("TPM_RESPECT_MANUAL_TPS", "true").strip().lower() == "true"
_TRAIL_R_MULT = Decimal(os.getenv("TPM_TRAIL_R_MULT", "1.0"))
SL_R_MULT = Decimal(os.getenv("TPM_SL_R_MULT", "2.2"))

TRAILING_ENABLED = os.getenv("TPM_TRAILING_ENABLED", "true").strip().lower() == "true"

try:
    _MIN_TP_GAP_TICKS = int(os.getenv("TPM_MIN_TP_GAP_TICKS", "5"))
except Exception:
    _MIN_TP_GAP_TICKS = 5

_ATR_CACHE_TTL = int(os.getenv("TPM_ATR_CACHE_SEC", "60"))
_ATR_CACHE: Dict[str, Tuple[float, Decimal]] = {}

_MANUAL_TP_MODE: Dict[str, bool] = {}
_MANUAL_SL_MODE: Dict[str, bool] = {}
_TRAIL_STATE: Dict[str, Dict[str, Decimal]] = {}

_LAST_SET_SL: Dict[str, Decimal] = {}

_EXIT_CACHE_TTL = int(os.getenv("TPM_EXIT_CACHE_SEC", "30"))
_EXIT_CACHE: Dict[str, Any] = {"ts": 0.0, "profiles": {}}

DEFAULT_EXIT_PROFILE_NAME = os.getenv("TPM_DEFAULT_EXIT_PROFILE", "standard_5").strip() or "standard_5"

TPM_STATUS_EVERY_SEC = int(os.getenv("TPM_STATUS_EVERY_SEC", "10"))
TPM_VERBOSE_STATUS = os.getenv("TPM_VERBOSE_STATUS", "true").strip().lower() == "true"

POSITIONS_BUS_PATH_ENV = os.getenv("POSITIONS_BUS_PATH", "")  # optional override

# ---- Phase 3 Step 1: infer parent trade_id from executions ----
_TRADE_ID_CACHE_TTL = int(os.getenv("TPM_TRADE_ID_CACHE_SEC", "30"))
_EXEC_LOOKBACK_LIMIT = int(os.getenv("TPM_EXEC_LOOKBACK_LIMIT", "80"))
_TRADE_ID_CACHE: Dict[str, Tuple[float, str]] = {}  # symbol -> (ts, trade_id)


def _project_root() -> Path:
    if settings is not None and getattr(settings, "ROOT", None) is not None:
        return Path(settings.ROOT)
    return Path(__file__).resolve().parents[2]


def _file_age_seconds(path: Path) -> Optional[float]:
    try:
        if not path.exists():
            return None
        return max(0.0, time.time() - path.stat().st_mtime)
    except Exception:
        return None


def _mask_key(s: Optional[str]) -> str:
    if not s:
        return "(missing)"
    s = str(s).strip()
    if len(s) <= 6:
        return "***"
    return f"{s[:3]}***{s[-3:]}"


def _cred_presence_snapshot() -> Dict[str, str]:
    return {
        "BYBIT_API_KEY": _mask_key(os.getenv("BYBIT_API_KEY")),
        "BYBIT_MAIN_API_KEY": _mask_key(os.getenv("BYBIT_MAIN_API_KEY")),
        "BYBIT_MAIN_WEBSOCKET_KEY": _mask_key(os.getenv("BYBIT_MAIN_WEBSOCKET_KEY")),
        "TG_BOT_TOKEN": "***" if os.getenv("TG_BOT_TOKEN") else "(missing)",
        "TG_CHAT_ID": "***" if os.getenv("TG_CHAT_ID") else "(missing)",
        "BYBIT_WS_PRIVATE_URL": os.getenv("BYBIT_WS_PRIVATE_URL", BYBIT_WS_PRIVATE_URL),
        "TPM_FORCE_TRADE_ID": _TPM_FORCE_TRADE_ID or "(none)",
    }


def _telemetry_paths(label: str) -> Tuple[Path, Path]:
    root = _project_root()
    state_dir = root / "state"
    positions_path = Path(POSITIONS_BUS_PATH_ENV) if POSITIONS_BUS_PATH_ENV.strip() else (state_dir / "positions_bus.json")
    hb_path = state_dir / f"ws_switchboard_heartbeat_{label}.txt"
    return positions_path, hb_path


def _print_boot_banner(mode: str, label: str) -> None:
    pos_path, hb_path = _telemetry_paths(label)
    creds = _cred_presence_snapshot()

    print("\n" + "=" * 80)
    print("[tp_sl_manager] BOOT")
    print("  version               : v6.16 + paper_overlay + phase3_trade_id_linked_tps + phase4_gate_hardened + console_proof")
    print(f"  ACCOUNT_LABEL         : {label}")
    print(f"  CATEGORY              : {CATEGORY}")
    print(f"  MODE                  : {mode}")
    print(f"  TPM_USE_WEBSOCKET     : {USE_WS}")
    print(f"  POLL_SECONDS          : {POLL_SECONDS}")
    print(f"  TRAILING_ENABLED      : {TRAILING_ENABLED}")
    print(f"  TPM_RESPECT_MANUAL_TPS: {_RESPECT_MANUAL_TPS}")
    print(f"  DEFAULT_EXIT_PROFILE  : {DEFAULT_EXIT_PROFILE_NAME}")
    print(f"  TPM_TRADE_ID_CACHE_SEC: {_TRADE_ID_CACHE_TTL}")
    print(f"  TPM_EXEC_LOOKBACK_LIMIT: {_EXEC_LOOKBACK_LIMIT}")
    print(f"  positions_bus.json    : {pos_path}")
    print(f"  ws_switchboard hb     : {hb_path}")
    print("  creds (sanitized)     :")
    for k, v in creds.items():
        print(f"    - {k}: {v}")
    print("=" * 80 + "\n")


def _infer_position_source(label: str, bus_max_age_sec: Optional[float]) -> Tuple[str, Optional[float], Optional[float]]:
    pos_path, hb_path = _telemetry_paths(label)
    pos_age = _file_age_seconds(pos_path)
    hb_age = _file_age_seconds(hb_path)

    if pos_age is None:
        return "REST_FALLBACK(or BUS missing)", pos_age, hb_age

    if bus_max_age_sec is None:
        return ("BUS_WS" if pos_age <= 30 else "REST_FALLBACK(likely)"), pos_age, hb_age

    if pos_age <= bus_max_age_sec:
        return "BUS_WS", pos_age, hb_age

    return "REST_FALLBACK(likely)", pos_age, hb_age


# ---------------------------------------------------------------------------
# PAPER overlay: read PaperBroker ledger into tp_sl_manager "positions" format
# ---------------------------------------------------------------------------

def _paper_ledger_path(account_label: str) -> Path:
    return _project_root() / "state" / "paper" / f"{account_label}.json"


def _paper_side_to_bus(side: Any) -> Optional[str]:
    s = str(side or "").strip().lower()
    if s in ("long", "buy"):
        return "buy"
    if s in ("short", "sell"):
        return "sell"
    return None


def _load_paper_positions(account_label: str) -> List[dict]:
    """
    Load PAPER open_positions for this ACCOUNT_LABEL from:
      state/paper/<account_label>.json

    Returns a list of dicts shaped like the bus positions expected by _ensure_exits_for_position().
    """
    path = _paper_ledger_path(account_label)
    if not path.exists():
        return []

    try:
        raw = json.loads(path.read_text(encoding="utf-8", errors="ignore") or "{}")
    except Exception:
        return []

    open_positions = raw.get("open_positions") or []
    if not isinstance(open_positions, list):
        return []

    out: List[dict] = []
    for pos in open_positions:
        if not isinstance(pos, dict):
            continue

        symbol = pos.get("symbol")
        side = _paper_side_to_bus(pos.get("side"))
        entry = pos.get("entry_price")
        size = pos.get("size")
        sl = pos.get("stop_price")
        trade_id = pos.get("trade_id")

        if not symbol or not side or entry in (None, "", 0, "0") or size in (None, "", 0, "0"):
            continue

        out.append(
            {
                "symbol": str(symbol),
                "side": str(side),
                "avgPrice": str(entry),
                "size": str(size),
                "stopLoss": str(sl) if sl not in (None, "", 0, "0") else None,
                "mode": "PAPER",
                "account_label": str(account_label),
                "trade_id": str(trade_id) if trade_id else None,
                "timeframe": pos.get("timeframe"),
                "setup_type": pos.get("setup_type"),
            }
        )

    return out


def _merge_positions(bus_positions: List[dict], paper_positions: List[dict]) -> List[dict]:
    """
    Merge lists without duplicating exact same trade_id records.
    If trade_id is missing, fall back to (symbol, side, avgPrice, size, account_label).
    """
    out: List[dict] = []
    seen: set = set()

    def key(p: dict) -> tuple:
        tid = p.get("trade_id") or p.get("client_trade_id")
        if tid:
            return ("trade_id", str(tid))
        return (
            "fallback",
            str(p.get("account_label") or ""),
            str(p.get("symbol") or ""),
            str(p.get("side") or ""),
            str(p.get("avgPrice") or ""),
            str(p.get("size") or ""),
        )

    for p in (bus_positions or []):
        if not isinstance(p, dict):
            continue
        k = key(p)
        if k in seen:
            continue
        seen.add(k)
        out.append(p)

    for p in (paper_positions or []):
        if not isinstance(p, dict):
            continue
        k = key(p)
        if k in seen:
            continue
        seen.add(k)
        out.append(p)

    return out


# ---------------------------------------------------------------------------
# Phase 4: Trade ID extraction (PAPER-friendly)
# ---------------------------------------------------------------------------

def _extract_trade_id_from_position(p: dict) -> Optional[str]:
    if not isinstance(p, dict):
        return None

    candidates = [
        "trade_id",
        "client_trade_id",
        "parent_trade_id",
        "entry_trade_id",
        "entry_order_link_id",
        "orderLinkId",
        "order_link_id",
        "entryOrderLinkId",
    ]

    for k in candidates:
        v = p.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s and s.lower() not in ("none", "null", "0"):
            return s

    return None


# ---------------------------------------------------------------------------
# Phase 3: Trade ID inference (parent link from executions) - LIVE
# ---------------------------------------------------------------------------

def _is_truthy(v: Any) -> bool:
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


def _side_to_bybit(side_now: str) -> str:
    return "Buy" if side_now.lower() == "buy" else "Sell"


def _infer_trade_id_from_executions(symbol: str, side_now: str) -> Optional[str]:
    now = time.time()
    cached = _TRADE_ID_CACHE.get(symbol)
    if cached is not None:
        ts, tid = cached
        if (now - ts) < _TRADE_ID_CACHE_TTL and tid:
            return tid

    try:
        res = bybit_get("/v5/execution/list", {"category": CATEGORY, "symbol": symbol, "limit": str(_EXEC_LOOKBACK_LIMIT)})
        rows = res.get("result", {}).get("list", []) or []
    except Exception as e:
        alert_bot_error("tp_sl_manager", f"{symbol} execution lookup failed: {e}", "WARN")
        return None

    want_side = _side_to_bybit(side_now)
    best_tid: Optional[str] = None
    best_ts: int = -1

    for ex in rows:
        try:
            ex_side = ex.get("side")
            if ex_side != want_side:
                continue

            ro = ex.get("reduceOnly")
            if ro is not None and _is_truthy(ro):
                continue

            lid = str(ex.get("orderLinkId") or "").strip()
            if not lid:
                continue

            ts_raw = ex.get("execTime") or ex.get("execTimestamp") or ex.get("time") or "0"
            try:
                ts_i = int(str(ts_raw))
            except Exception:
                ts_i = 0

            if ts_i > best_ts:
                best_ts = ts_i
                best_tid = lid
        except Exception:
            continue

    if best_tid:
        _TRADE_ID_CACHE[symbol] = (now, best_tid)
        return best_tid

    return None


def _tp_link_id(trade_id: str, idx: int) -> str:
    return f"{trade_id}:TP{idx}"


# ---------------------------------------------------------------------------
# Phase 4: Gate helper (skip TP/SL sync if blocked)
# ---------------------------------------------------------------------------

def _gate_allows_trade(symbol: str, trade_id: Optional[str]) -> Tuple[bool, str]:
    if enforce_decision is None:
        return True, "enforcer_missing"
    if not trade_id:
        return True, "no_trade_id"

    try:
        verdict = enforce_decision(str(trade_id))
    except Exception as e:
        # Fail-open: do not brick exits on tooling error
        return True, f"enforcer_error:{e}"

    allow = bool(verdict.get("allow", False))
    reason = str(verdict.get("reason") or verdict.get("decision_code") or "blocked").strip()

    if allow:
        return True, reason

    now = time.time()
    last = float(_GATE_BLOCK_LAST.get(symbol, 0.0) or 0.0)
    if (now - last) >= _GATE_BLOCK_THROTTLE_SEC:
        _GATE_BLOCK_LAST[symbol] = now

        # ✅ Deterministic console proof (this is what you were missing)
        print(
            f"[tp_sl_manager] 🚫 GATE_BLOCKED symbol={symbol} trade_id={trade_id} reason={reason}",
            flush=True,
        )

        # Existing telemetry
        alert_bot_error(
            "tp_sl_manager",
            f"🚫 Gate blocked TP/SL sync for {symbol} trade_id={trade_id} reason={reason}",
            "WARN",
        )
        try:
            send_tg(f"🚫 AI gate blocked TP/SL sync for {symbol} (trade_id={trade_id}) reason={reason}")
        except Exception:
            pass

    return False, reason


# ---------------------------------------------------------------------------
# Exit profiles loader
# ---------------------------------------------------------------------------

def _load_exit_profiles() -> Dict[str, dict]:
    now = time.time()
    ts = float(_EXIT_CACHE.get("ts", 0.0) or 0.0)
    if now - ts < _EXIT_CACHE_TTL and isinstance(_EXIT_CACHE.get("profiles"), dict):
        return _EXIT_CACHE["profiles"]

    path = _project_root() / "config" / "exit_profiles.yaml"
    if not path.exists():
        alert_bot_error("tp_sl_manager", f"Missing exit_profiles.yaml at {path}", "ERROR")
        _EXIT_CACHE["ts"] = now
        _EXIT_CACHE["profiles"] = {}
        return {}

    try:
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        if not isinstance(data, dict):
            raise ValueError("exit_profiles.yaml root must be a dict")
        profiles = data.get("profiles") or {}
        if not isinstance(profiles, dict):
            raise ValueError("exit_profiles.yaml must contain 'profiles:' mapping")
        out = {str(k): v for k, v in profiles.items() if isinstance(v, dict)}
        _EXIT_CACHE["ts"] = now
        _EXIT_CACHE["profiles"] = out
        return out
    except Exception as e:
        alert_bot_error("tp_sl_manager", f"Failed to load exit_profiles.yaml: {e}", "ERROR")
        _EXIT_CACHE["ts"] = now
        _EXIT_CACHE["profiles"] = {}
        return {}


def _open_orders(symbol: str) -> List[dict]:
    r = bybit_get("/v5/order/realtime", {"category": CATEGORY, "symbol": symbol})
    return r.get("result", {}).get("list", []) or []


def _tp_orders(orders: List[dict], side_now: str) -> List[dict]:
    opp = "Sell" if side_now.lower() == "buy" else "Buy"
    return [
        o for o in orders
        if o.get("orderType") == "Limit"
        and o.get("side") == opp
        and str(o.get("reduceOnly", "False")).lower() == "true"
        and o.get("orderStatus") in ("New", "PartiallyFilled")
    ]


def _get_atr(symbol: str, entry: Decimal) -> Decimal:
    now = time.time()
    cached = _ATR_CACHE.get(symbol)
    if cached is not None:
        ts, val = cached
        if now - ts < _ATR_CACHE_TTL:
            return val

    atr_val = atr14(symbol, interval="60")
    if atr_val <= 0:
        atr_val = entry * Decimal("0.002")

    _ATR_CACHE[symbol] = (now, atr_val)
    return atr_val


def _compute_R_base(symbol: str, entry: Decimal) -> Decimal:
    tick, _step, _min_notional = get_ticks(symbol)
    atr = _get_atr(symbol, entry)
    if atr <= 0:
        atr = entry * Decimal("0.002")

    R_base = atr * Decimal(ATR_MULT)
    min_R = tick * Decimal(R_MIN_TICKS)
    if R_base < min_R:
        R_base = min_R
    return R_base


def _safe_tp_price(symbol: str, side_now: str, target_px: Decimal) -> Decimal:
    try:
        if _MIN_TP_GAP_TICKS <= 0:
            return target_px

        mkt = Decimal(str(last_price(symbol)))
        if mkt <= 0:
            return target_px

        tick, _step, _ = get_ticks(symbol)
        gap = tick * Decimal(_MIN_TP_GAP_TICKS)

        if side_now.lower() == "buy":
            min_px = mkt + gap
            if target_px <= min_px:
                target_px = min_px
        else:
            max_px = mkt - gap
            if target_px >= max_px:
                target_px = max_px

        return psnap(target_px, tick)
    except Exception:
        return target_px


def _compute_trailing_sl(symbol: str, side_now: str, entry: Decimal, base_sl: Decimal, first_r_dist: Decimal) -> Decimal:
    if not TRAILING_ENABLED or _TRAIL_R_MULT <= 0:
        return base_sl

    try:
        price = Decimal(str(last_price(symbol)))
    except Exception:
        return base_sl

    if price <= 0 or first_r_dist <= 0:
        return base_sl

    trail_dist = first_r_dist * _TRAIL_R_MULT

    state = _TRAIL_STATE.get(symbol)
    if state is None or state.get("entry") != entry or state.get("base_sl") != base_sl:
        state = {"entry": entry, "base_sl": base_sl, "best": price}
    else:
        best = state.get("best", entry)
        if side_now.lower() == "buy":
            if price > best:
                best = price
        else:
            if price < best:
                best = price
        state["best"] = best

    best = state["best"]

    if side_now.lower() == "buy":
        sl_candidate = best - trail_dist
        sl_new = max(base_sl, sl_candidate)
    else:
        sl_candidate = best + trail_dist
        sl_new = min(base_sl, sl_candidate)

    tick, _step, _ = get_ticks(symbol)
    sl_new = psnap(sl_new, tick)
    _TRAIL_STATE[symbol] = state
    return sl_new


def _amend_tp_order(symbol: str, order: dict, new_qty: Optional[Decimal], new_price: Optional[Decimal], side_now: Optional[str] = None) -> None:
    body: Dict[str, str] = {"category": CATEGORY, "symbol": symbol}

    order_id = order.get("orderId")
    link_id = order.get("orderLinkId")
    if order_id:
        body["orderId"] = order_id
    elif link_id:
        body["orderLinkId"] = link_id
    else:
        return

    if new_price is not None:
        if side_now is not None:
            new_price = _safe_tp_price(symbol, side_now, new_price)
        body["price"] = str(new_price)
    if new_qty is not None:
        body["qty"] = str(new_qty)

    try:
        bybit_post("/v5/order/amend", body)
    except Exception as e:
        alert_bot_error("tp_sl_manager", f"{symbol} amend error: {e}", "ERROR")


def _cancel_tp_order(symbol: str, order: dict) -> None:
    body: Dict[str, str] = {"category": CATEGORY, "symbol": symbol}
    order_id = order.get("orderId")
    link_id = order.get("orderLinkId")
    if order_id:
        body["orderId"] = order_id
    elif link_id:
        body["orderLinkId"] = link_id
    else:
        return

    try:
        bybit_post("/v5/order/cancel", body)
    except Exception as e:
        alert_bot_error("tp_sl_manager", f"{symbol} cancel error: {e}", "WARN")


def _extract_existing_sl(p: dict) -> Optional[Decimal]:
    raw = (
        p.get("stopLoss")
        or p.get("stopLossPrice")
        or p.get("slPrice")
        or p.get("stop_loss")
    )
    if raw in (None, "", "0", 0):
        return None
    try:
        return Decimal(str(raw))
    except Exception:
        return None


def _strategy_exit_profile_from_any(strategy_obj: Any) -> Optional[str]:
    if isinstance(strategy_obj, dict):
        ep = strategy_obj.get("exit_profile") or strategy_obj.get("exitProfile")
        if isinstance(ep, str) and ep.strip():
            return ep.strip()
        return None

    try:
        ep = getattr(strategy_obj, "exit_profile", None)
        if isinstance(ep, str) and ep.strip():
            return ep.strip()
    except Exception:
        pass

    return None


def _get_exit_profile_name_for_position(p: dict) -> str:
    account_label = (p.get("account_label") or p.get("label") or p.get("account_label_slug") or "").strip()

    sub_uid_raw = (p.get("sub_uid") or p.get("subAccountId") or p.get("accountId") or p.get("subId"))
    sub_uid = str(sub_uid_raw) if sub_uid_raw not in (None, "") else ""

    is_main = False
    if not sub_uid and account_label.lower() in ("", "main", "unified_main", "primary"):
        is_main = True
    if is_main:
        return "standard_10"

    if strat_mod is None:
        return DEFAULT_EXIT_PROFILE_NAME

    strategy_obj = None
    try:
        if sub_uid and hasattr(strat_mod, "get_strategy_for_sub"):
            strategy_obj = strat_mod.get_strategy_for_sub(sub_uid)  # type: ignore
    except Exception:
        strategy_obj = None

    if strategy_obj is None:
        try:
            if sub_uid and hasattr(strat_mod, "get_strategy_by_sub_uid"):
                strategy_obj = strat_mod.get_strategy_by_sub_uid(sub_uid)  # type: ignore
        except Exception:
            strategy_obj = None

    if strategy_obj is None:
        try:
            if account_label and hasattr(strat_mod, "get_strategy_for_label"):
                strategy_obj = strat_mod.get_strategy_for_label(account_label)  # type: ignore
        except Exception:
            strategy_obj = None

    ep = _strategy_exit_profile_from_any(strategy_obj)
    if ep:
        return ep

    return DEFAULT_EXIT_PROFILE_NAME


def _compute_exits_from_profile(symbol: str, side_now: str, entry: Decimal, profile_name: str) -> Tuple[Decimal, List[Decimal], List[Decimal]]:
    profiles = _load_exit_profiles()
    prof = profiles.get(profile_name)

    if not isinstance(prof, dict):
        alert_bot_error(
            "tp_sl_manager",
            f"{symbol}: unknown exit_profile '{profile_name}', using '{DEFAULT_EXIT_PROFILE_NAME}'",
            "WARN",
        )
        prof = profiles.get(DEFAULT_EXIT_PROFILE_NAME)

    if not isinstance(prof, dict):
        raise RuntimeError("No valid exit profiles loaded (exit_profiles.yaml broken/missing)")

    tps = prof.get("tps") or []
    sl = prof.get("sl") or {}

    if not isinstance(tps, list) or not isinstance(sl, dict):
        raise ValueError(f"exit_profile '{profile_name}' invalid shape")

    tick, _step, _ = get_ticks(symbol)
    atr = _get_atr(symbol, entry)
    if atr <= 0:
        atr = entry * Decimal("0.002")

    R_base = _compute_R_base(symbol, entry)

    max_tp_dist_atr = atr * Decimal(TP5_MAX_ATR_MULT)
    max_tp_dist_pct = entry * (Decimal(TP5_MAX_PCT) / Decimal(100))
    max_tp_cap = min(max_tp_dist_atr, max_tp_dist_pct)

    rr_list: List[Decimal] = []
    sz_list: List[Decimal] = []
    for tp in tps[:CORE_TP_COUNT]:
        if not isinstance(tp, dict):
            continue
        rr = tp.get("rr")
        sz = tp.get("size_pct")
        try:
            rr_d = Decimal(str(rr))
            sz_d = Decimal(str(sz))
        except Exception:
            continue
        if rr_d <= 0 or sz_d <= 0:
            continue
        rr_list.append(rr_d)
        sz_list.append(sz_d)

    if not rr_list:
        rr_list = [Decimal("1.0")]
        sz_list = [Decimal("1.0")]

    sz_sum = sum(sz_list)
    if sz_sum <= 0:
        sz_list = [Decimal("1.0") / Decimal(len(sz_list)) for _ in sz_list]
    else:
        sz_list = [s / sz_sum for s in sz_list]

    furthest_rr = max(rr_list)
    natural_furthest = furthest_rr * R_base
    scale = Decimal("1.0")
    if natural_furthest > max_tp_cap and max_tp_cap > 0:
        scale = max_tp_cap / natural_furthest

    sl_rr_raw = sl.get("rr", -1.0)
    try:
        sl_rr = Decimal(str(sl_rr_raw))
    except Exception:
        sl_rr = Decimal("-1.0")
    if sl_rr >= 0:
        sl_rr = -abs(sl_rr) if sl_rr != 0 else Decimal("-1.0")

    sl_dist = abs(sl_rr) * R_base * SL_R_MULT

    if side_now.lower() == "buy":
        sl_px = entry - sl_dist
        tp_prices = [entry + (rr * R_base * scale) for rr in rr_list]
        tp_prices = sorted(tp_prices)
    else:
        sl_px = entry + sl_dist
        tp_prices = [entry - (rr * R_base * scale) for rr in rr_list]
        tp_prices = sorted(tp_prices, reverse=True)

    sl_px = psnap(sl_px, tick)
    tp_prices = [psnap(px, tick) for px in tp_prices]

    return sl_px, tp_prices, sz_list


def _detect_manual_override(symbol: str, side_now: str, tpo: List[dict], target_tps: List[Decimal]) -> bool:
    if not tpo or not target_tps:
        return False

    tick, _step, _ = get_ticks(symbol)

    try:
        cur = [Decimal(str(o.get("price", "0"))) for o in tpo]
        cur = [c for c in cur if c > 0]
    except Exception:
        return False

    if side_now.lower() == "buy":
        cur_sorted = sorted(cur)
        tgt_sorted = sorted(target_tps)
    else:
        cur_sorted = sorted(cur, reverse=True)
        tgt_sorted = sorted(target_tps, reverse=True)

    n = min(len(cur_sorted), len(tgt_sorted))
    if n <= 0:
        return False

    mismatches = 0
    for i in range(n):
        if abs(cur_sorted[i] - tgt_sorted[i]) > (tick * 2):
            mismatches += 1
    return mismatches >= 2


def _split_qty_by_pcts(size: Decimal, pcts: List[Decimal], step: Decimal) -> List[Decimal]:
    if size <= 0 or not pcts:
        return []

    target_total = qdown(size, step)
    if target_total <= 0:
        return []

    qtys = [qdown(target_total * pct, step) for pct in pcts]
    s = sum(qtys)

    rem = target_total - s
    if rem > 0 and qtys:
        qtys[-1] = qdown(qtys[-1] + rem, step)

    if qtys and qtys[-1] <= 0:
        qtys[-1] = qdown(target_total - sum(qtys[:-1]), step)

    return qtys


def _resolve_trade_id(symbol: str, side_now: str, position_trade_id: Optional[str]) -> Optional[str]:
    if _TPM_FORCE_TRADE_ID:
        return _TPM_FORCE_TRADE_ID
    if position_trade_id:
        return position_trade_id
    return _infer_trade_id_from_executions(symbol, side_now)


def _sync_tp_ladder(symbol: str, side_now: str, size: Decimal, target_tps: List[Decimal], target_qtys: List[Decimal], position_trade_id: Optional[str]) -> None:
    tick, step, _ = get_ticks(symbol)

    pairs = [(px, q) for px, q in zip(target_tps, target_qtys) if q > 0]
    if not pairs:
        return

    tps = [px for px, _ in pairs]
    qtys = [q for _, q in pairs]

    orders_all = _open_orders(symbol)
    tpo = _tp_orders(orders_all, side_now)

    trade_id = _resolve_trade_id(symbol, side_now, position_trade_id)

    
    allow, _reason = _gate_allows_trade(symbol, trade_id)
    if not allow:
        return

    if not tpo:
        _MANUAL_TP_MODE.pop(symbol, None)
        if tps:
            base_safe = _safe_tp_price(symbol, side_now, tps[0])
            delta = base_safe - tps[0]
            tps = [psnap(px + delta, tick) for px in tps]

        for idx, (px, q) in enumerate(zip(tps, qtys), start=1):
            try:
                lid = _tp_link_id(trade_id, idx) if trade_id else None
                place_reduce_tp(symbol, side_now, q, px, link_id=lid)
            except Exception as e:
                alert_bot_error("tp_sl_manager", f"{symbol} TP create error: {e}", "WARN")
        return

    manual_mode = _MANUAL_TP_MODE.get(symbol, False)

    if _RESPECT_MANUAL_TPS and not manual_mode:
        if _detect_manual_override(symbol, side_now, tpo, tps):
            manual_mode = True
            _MANUAL_TP_MODE[symbol] = True
            try:
                send_tg(
                    f"✋ Manual TP override detected for {symbol}. "
                    f"Bot will respect your TP prices until you cancel them or flatten."
                )
            except Exception:
                pass

    if manual_mode and _RESPECT_MANUAL_TPS:
        n = len(tpo)
        if n <= 0:
            _MANUAL_TP_MODE.pop(symbol, None)
            return

        each = qdown(size / Decimal(n), step)
        if each <= 0:
            return

        for o in tpo:
            try:
                cur_qty = Decimal(str(o.get("qty", "0")))
            except Exception:
                cur_qty = Decimal("0")
            if cur_qty != each:
                _amend_tp_order(symbol, o, new_qty=each, new_price=None, side_now=None)
        return

    if tps:
        base_safe = _safe_tp_price(symbol, side_now, tps[0])
        delta = base_safe - tps[0]
        tps = [psnap(px + delta, tick) for px in tps]

    try:
        if side_now.lower() == "buy":
            tpo_sorted = sorted(tpo, key=lambda o: Decimal(str(o.get("price", "0"))))
        else:
            tpo_sorted = sorted(tpo, key=lambda o: Decimal(str(o.get("price", "0"))), reverse=True)
    except Exception:
        tpo_sorted = tpo

    n_common = min(len(tpo_sorted), len(tps))
    for i in range(n_common):
        _amend_tp_order(symbol, tpo_sorted[i], new_qty=qtys[i], new_price=tps[i], side_now=side_now)

    if len(tpo_sorted) > len(tps):
        for o in tpo_sorted[len(tps):]:
            _cancel_tp_order(symbol, o)

    if len(tps) > len(tpo_sorted):
        for idx, (px, q) in enumerate(zip(tps[len(tpo_sorted):], qtys[len(tpo_sorted):]), start=len(tpo_sorted) + 1):
            try:
                lid = _tp_link_id(trade_id, idx) if trade_id else None
                place_reduce_tp(symbol, side_now, q, px, link_id=lid)
            except Exception as e:
                alert_bot_error("tp_sl_manager", f"{symbol} TP create (extra rung) error: {e}", "WARN")


def _ensure_exits_for_position(p: dict, seen_state: Dict[str, Tuple[Decimal, Decimal, Decimal]]) -> None:
    symbol = p["symbol"]
    side_now = p["side"]
    entry = Decimal(str(p["avgPrice"]))
    size = Decimal(str(p["size"]))

    if size <= 0:
        seen_state.pop(symbol, None)
        _MANUAL_TP_MODE.pop(symbol, None)
        _MANUAL_SL_MODE.pop(symbol, None)
        _TRAIL_STATE.pop(symbol, None)
        _LAST_SET_SL.pop(symbol, None)
        return

    position_trade_id = _extract_trade_id_from_position(p)
    trade_id = _resolve_trade_id(symbol, side_now, position_trade_id)

    allow, _reason = _gate_allows_trade(symbol, trade_id)
    if not allow:
        return

    profile_name = _get_exit_profile_name_for_position(p)

    base_sl, tp_prices, tp_pcts = _compute_exits_from_profile(symbol, side_now, entry, profile_name)
    tick, step, _ = get_ticks(symbol)

    tp_qtys = _split_qty_by_pcts(size, tp_pcts, step)

    existing_sl = _extract_existing_sl(p)
    manual_sl_mode = _MANUAL_SL_MODE.get(symbol, False)

    if existing_sl is not None:
        if not manual_sl_mode:
            try:
                if abs(existing_sl - base_sl) > (tick * 2):
                    manual_sl_mode = True
                    _MANUAL_SL_MODE[symbol] = True
                    try:
                        send_tg(
                            f"✋ Manual SL override detected for {symbol}. "
                            f"Bot will respect your SL until you flatten."
                        )
                    except Exception:
                        pass
            except Exception:
                pass
    else:
        if manual_sl_mode:
            _MANUAL_SL_MODE.pop(symbol, None)
            manual_sl_mode = False

    first_r_dist = abs(tp_prices[0] - entry) if tp_prices else _compute_R_base(symbol, entry)

    if manual_sl_mode and existing_sl is not None:
        sl_effective = existing_sl
    else:
        sl_effective = _compute_trailing_sl(symbol, side_now, entry, base_sl, first_r_dist)
        last_set = _LAST_SET_SL.get(symbol)
        if last_set is None or abs(last_set - sl_effective) > (tick * 1):
            set_stop_loss(symbol, sl_effective)
            _LAST_SET_SL[symbol] = sl_effective

    _sync_tp_ladder(symbol, side_now, size, tp_prices, tp_qtys, position_trade_id=position_trade_id)

    prev = seen_state.get(symbol)
    state = (entry, size, sl_effective)

    if prev != state:
        try:
            used = [f"{px}({q})" for px, q in zip(tp_prices, tp_qtys)]
            send_tg(
                f"🎯 Exits set {symbol} {side_now} | size {size} | "
                f"profile {profile_name} | SL {sl_effective} | "
                f"TPs {', '.join(used)}"
            )
        except Exception:
            pass

    seen_state[symbol] = state


def _loop_http_poll() -> None:
    label = os.getenv("ACCOUNT_LABEL", "main").strip() or "main"
    _print_boot_banner(mode="HTTP + position_bus (with REST fallback) + PAPER overlay", label=label)

    try:
        send_tg(f"🎛 Flashback TP/SL Manager ONLINE (HTTP+position_bus+PAPER overlay, label={label}, poll={POLL_SECONDS}s).")
    except Exception:
        pass

    seen: Dict[str, Tuple[Decimal, Decimal, Decimal]] = {}
    last_status_print = 0.0

    BUS_MAX_AGE_SEC = float(os.getenv("TPM_BUS_MAX_AGE_SEC", "10"))

    while True:
        record_heartbeat("tp_sl_manager")
        try:
            bus_positions = bus_get_positions_snapshot(
                label=label,
                category=CATEGORY,
                max_age_seconds=int(BUS_MAX_AGE_SEC),
                allow_rest_fallback=True,
            )

            paper_positions = _load_paper_positions(label)
            positions = _merge_positions(bus_positions, paper_positions)

            now = time.time()
            if TPM_VERBOSE_STATUS and (now - last_status_print) >= TPM_STATUS_EVERY_SEC:
                src, pos_age, hb_age = _infer_position_source(label, bus_max_age_sec=BUS_MAX_AGE_SEC)
                print(
                    f"[tp_sl_manager] status | label={label} | mode=HTTP | source={src}+PAPER | "
                    f"positions_bus_age={('MISSING' if pos_age is None else f'{pos_age:.2f}s')} | "
                    f"ws_hb_age={('MISSING' if hb_age is None else f'{hb_age:.2f}s')} | "
                    f"positions={len(positions)} (bus={len(bus_positions)}, paper={len(paper_positions)})"
                )
                last_status_print = now

            current_symbols = set()
            for p in positions:
                symbol = p.get("symbol")
                if not symbol:
                    continue
                current_symbols.add(symbol)
                _ensure_exits_for_position(p, seen_state=seen)

            for s in list(seen.keys()):
                if s not in current_symbols:
                    seen.pop(s, None)
                    _MANUAL_TP_MODE.pop(s, None)
                    _MANUAL_SL_MODE.pop(s, None)
                    _TRAIL_STATE.pop(s, None)
                    _LAST_SET_SL.pop(s, None)

            time.sleep(POLL_SECONDS)

        except Exception as e:
            alert_bot_error("tp_sl_manager", f"HTTP loop error: {e}", "ERROR")
            time.sleep(5)


def _handle_ws_position_message(msg: dict, seen: Dict[str, Tuple[Decimal, Decimal, Decimal]]) -> None:
    topic = msg.get("topic", "")
    if "position" not in topic:
        return

    data = msg.get("data", [])
    if isinstance(data, dict):
        data = [data]

    current_symbols = set()

    for p in data:
        if str(p.get("category", "")).lower() != CATEGORY:
            continue
        symbol = p.get("symbol")
        if not symbol:
            continue
        current_symbols.add(symbol)

        size = Decimal(str(p.get("size", "0")))
        if size <= 0:
            seen.pop(symbol, None)
            _MANUAL_TP_MODE.pop(symbol, None)
            _MANUAL_SL_MODE.pop(symbol, None)
            _TRAIL_STATE.pop(symbol, None)
            _LAST_SET_SL.pop(symbol, None)
            continue

        norm = {
            "symbol": symbol,
            "side": p.get("side"),
            "avgPrice": p.get("avgPrice"),
            "size": p.get("size"),
            "stopLoss": p.get("stopLoss") or p.get("stopLossPrice") or p.get("slPrice"),
            "sub_uid": p.get("sub_uid") or p.get("subAccountId") or p.get("accountId") or p.get("subId"),
            "trade_id": p.get("trade_id") or p.get("client_trade_id") or p.get("orderLinkId") or p.get("entry_order_link_id"),
        }
        _ensure_exits_for_position(norm, seen_state=seen)

    for s in list(seen.keys()):
        if s not in current_symbols:
            seen.pop(s, None)
            _MANUAL_TP_MODE.pop(s, None)
            _MANUAL_SL_MODE.pop(s, None)
            _TRAIL_STATE.pop(s, None)
            _LAST_SET_SL.pop(s, None)


def _loop_ws() -> None:
    if websocket is None:
        raise RuntimeError("websocket-client is not installed. pip install websocket-client")

    label = os.getenv("ACCOUNT_LABEL", "main").strip() or "main"
    _print_boot_banner(mode="DIRECT WS (private: position)", label=label)

    try:
        send_tg(f"🎛 Flashback TP/SL Manager ONLINE (WebSocket mode, label={label}).")
    except Exception:
        pass

    seen: Dict[str, Tuple[Decimal, Decimal, Decimal]] = {}
    last_status_print = 0.0

    while True:
        ws = None
        try:
            ws = websocket.create_connection(BYBIT_WS_PRIVATE_URL, timeout=5)

            auth_msg = build_ws_auth_payload_main()
            ws.send(json.dumps(auth_msg))

            raw = ws.recv()
            resp = json.loads(raw)
            if resp.get("success") is False or resp.get("retCode", 0) != 0:
                raise RuntimeError(f"WS auth failed: {resp}")

            ws.send(json.dumps({"op": "subscribe", "args": ["position"]}))

            last_ping = time.time()

            while True:
                record_heartbeat("tp_sl_manager")

                now = time.time()
                if TPM_VERBOSE_STATUS and (now - last_status_print) >= TPM_STATUS_EVERY_SEC:
                    print(f"[tp_sl_manager] status | label={label} | mode=WS | connected=true")
                    last_status_print = now

                if now - last_ping > 15:
                    ws.send(json.dumps({"op": "ping"}))
                    last_ping = now

                raw = ws.recv()
                if not raw:
                    raise RuntimeError("WS closed")

                msg = json.loads(raw)
                if msg.get("op") in ("pong", "ping"):
                    continue
                if "topic" in msg and "position" in msg["topic"]:
                    _handle_ws_position_message(msg, seen=seen)

        except Exception as e:
            alert_bot_error("tp_sl_manager", f"WS loop error: {e}", "ERROR")
            time.sleep(3)
        finally:
            if ws is not None:
                try:
                    ws.close()
                except Exception:
                    pass


def loop() -> None:
    if USE_WS:
        try:
            _loop_ws()
        except Exception as e:
            alert_bot_error("tp_sl_manager", f"WS hard failure, falling back to HTTP: {e}", "ERROR")
            _loop_http_poll()
    else:
        _loop_http_poll()


if __name__ == "__main__":
    loop()
