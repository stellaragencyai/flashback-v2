# app/bots/tp_sl_manager.py
# Flashback — TP/SL Manager v6.10 (10-TP capable, main=standard_10)
#
# Mode summary
# ------------
# HTTP mode:
#   - Uses app.core.position_bus.get_positions_snapshot(...)
#   - That prefers WS-fed state/positions_bus.json if fresh
#   - Falls back to REST list_open_positions(category="linear") for label "main"
#
# WS mode (unchanged):
#   - Connects directly to BYBIT_WS_PRIVATE_URL
#   - Subscribes to "position" private topic
#   - Feeds positions from WS pushes only
#
# Keeps:
#   - Exit profiles (standard_10 / standard_7 / standard_5 / aggressive_7 / scalp_3)
#   - Up to 10-TP ladders, manual TP/SL override logic
#   - Trailing SL, ATR-based spacing, safety gap from market
#   - HTTP polling + optional direct WS mode

import os
import time
import json
from decimal import Decimal
from typing import Dict, Tuple, List, Optional

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

# Position bus (HTTP mode reads positions via this)
from app.core.position_bus import get_positions_snapshot as bus_get_positions_snapshot

# Optional websocket support (websocket-client) for direct TP/SL WS mode
try:
    import websocket  # type: ignore
except ImportError:
    websocket = None

# Strategy registry (for per-sub exit profiles)
try:
    from app.core import strategies as strat_mod  # expects get_strategy_for_sub(sub_uid)
except Exception:
    strat_mod = None  # type: ignore

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

# Max number of TP rungs the engine supports.
# Profiles (standard_5, standard_7, standard_10, etc.) choose how many they use.
CORE_TP_COUNT = 10

# Polling cadence (HTTP mode only + safety watchdog)
POLL_SECONDS = int(os.getenv("TPM_POLL_SECONDS", "2"))

# WebSocket toggle (direct private WS for TP/SL manager itself)
USE_WS = os.getenv("TPM_USE_WEBSOCKET", "false").strip().lower() == "true"

# Respect manual TP modifications (prices) or not
_RESPECT_MANUAL_TPS = os.getenv("TPM_RESPECT_MANUAL_TPS", "true").strip().lower() == "true"

# Trailing SL config
_TRAIL_R_MULT = Decimal(os.getenv("TPM_TRAIL_R_MULT", "1.0"))

# SL distance multiplier (relative to base R)
SL_R_MULT = Decimal(os.getenv("TPM_SL_R_MULT", "2.2"))

# Minimum TP gap in ticks from current price for auto-managed TPs
try:
    _MIN_TP_GAP_TICKS = int(os.getenv("TPM_MIN_TP_GAP_TICKS", "5"))
except Exception:
    _MIN_TP_GAP_TICKS = 5

# ATR cache: symbol -> (ts, atr)
_ATR_CACHE_TTL = int(os.getenv("TPM_ATR_CACHE_SEC", "60"))
_ATR_CACHE: Dict[str, Tuple[float, Decimal]] = {}

# Manual TP override per symbol: if True, we do NOT amend TP prices for that symbol.
_MANUAL_TP_MODE: Dict[str, bool] = {}

# Manual SL override per symbol: if True, we do NOT call set_stop_loss for that symbol.
_MANUAL_SL_MODE: Dict[str, bool] = {}

# Trailing SL state per symbol:
#   symbol -> {
#       "entry": Decimal,
#       "base_sl": Decimal,
#       "best": Decimal,
#   }
_TRAIL_STATE: Dict[str, Dict[str, Decimal]] = {}

# Default exit profile (used if strategy lookup fails or is absent)
# NOTE:
#   - Main account is explicitly overridden to standard_10 in _get_exit_profile_for_position.
#   - This default mostly applies to subs without configured exit_profile.
DEFAULT_EXIT_PROFILE = {
    "name": "standard_7",
    "tp_count": 7,
    "trailing_sl": True,
}


def _open_orders(symbol: str) -> List[dict]:
    r = bybit_get("/v5/order/realtime", {"category": CATEGORY, "symbol": symbol})
    return r.get("result", {}).get("list", []) or []


def _tp_orders(orders: List[dict], side_now: str) -> List[dict]:
    # TP = reduce-only limits on opposite side
    opp = "Sell" if side_now.lower() == "buy" else "Buy"
    return [
        o for o in orders
        if o.get("orderType") == "Limit"
        and o.get("side") == opp
        and str(o.get("reduceOnly", "False")).lower() == "true"
        and o.get("orderStatus") in ("New", "PartiallyFilled")
    ]


def _get_atr(symbol: str, entry: Decimal) -> Decimal:
    """
    Cached ATR(14) on 1h. Fallback to synthetic 0.2% R if missing.
    """
    now = time.time()
    cached = _ATR_CACHE.get(symbol)
    if cached is not None:
        ts, val = cached
        if now - ts < _ATR_CACHE_TTL:
            return val

    atr_val = atr14(symbol, interval="60")
    if atr_val <= 0:
        # Fallback: synthetic ~0.2% band; we'll transform into R later.
        atr_val = entry * Decimal("0.002")

    _ATR_CACHE[symbol] = (now, atr_val)
    return atr_val


def _compute_exit_grid(
    symbol: str,
    side_now: str,
    entry: Decimal,
    tp_count: int,
) -> Tuple[Decimal, List[Decimal]]:
    """
    Returns (stop_loss_price, [tp1..tpN]) snapped to valid tick.

    Spacing logic (simplified but still ATR-aware):
      - Base R_base = max(ATR * ATR_MULT, R_MIN_TICKS * tick)
      - SL distance uses R_sl = R_base * SL_R_MULT  (wider stop)
      - We compute a maximum TP distance:
          • max_tp_dist_atr = ATR * TP5_MAX_ATR_MULT
          • max_tp_dist_pct = entry * (TP5_MAX_PCT / 100)
        and choose the smaller of the two.
      - Grid is then **evenly spaced** from entry out to max_dist / or tp_count * R_base,
        whichever is tighter.
      - This guarantees equal spacing v1: each rung is the same distance apart.
    """
    if tp_count <= 0:
        tp_count = 1
    if tp_count > CORE_TP_COUNT:
        tp_count = CORE_TP_COUNT

    tick, _step, _min_notional = get_ticks(symbol)

    atr = _get_atr(symbol, entry)
    if atr <= 0:
        atr = entry * Decimal("0.002")

    # Base R distance
    R_base = atr * Decimal(ATR_MULT)

    # Enforce minimum ticks
    min_R = tick * Decimal(R_MIN_TICKS)
    if R_base < min_R:
        R_base = min_R

    # Separate distances for TP vs SL
    R_sl = R_base * SL_R_MULT

    # Cap furthest TP
    max_tp_dist_atr = atr * Decimal(TP5_MAX_ATR_MULT)
    max_tp_dist_pct = entry * (Decimal(TP5_MAX_PCT) / Decimal(100))
    max_tp_dist_cap = min(max_tp_dist_atr, max_tp_dist_pct)

    # Natural grid if we don't hit the cap
    natural_max_dist = R_base * Decimal(tp_count)
    max_dist = min(natural_max_dist, max_tp_dist_cap)

    # Ensure we don't end up with zero distance
    if max_dist <= 0:
        max_dist = R_base * Decimal(tp_count)

    step = max_dist / Decimal(tp_count)

    if side_now.lower() == "buy":
        sl = entry - R_sl
        tps = [entry + step * Decimal(i) for i in range(1, tp_count + 1)]
    else:
        sl = entry + R_sl
        tps = [entry - step * Decimal(i) for i in range(1, tp_count + 1)]

    # Snap to tick
    sl = psnap(sl, tick)
    tps = [psnap(px, tick) for px in tps]
    return sl, tps


def _get_exit_profile_for_position(p: dict) -> Dict[str, object]:
    """
    Determine exit profile for a given position using strategy config when possible.

    Supports two shapes in strategies.yaml:

      exit_profile:
        name: standard_7
        tp_count: 7
        trailing_sl: true

      exit_profile: "standard_5"

    Rules:
      - MAIN account (no sub_uid OR account_label == "main"):
          -> standard_10 (10 TP ladder, trailing SL)
      - Subaccounts:
          -> use strategy exit_profile if available
          -> otherwise fall back to DEFAULT_EXIT_PROFILE
    """
    profile: Dict[str, object] = dict(DEFAULT_EXIT_PROFILE)

    # Detect main account: no sub_uid AND account_label/main-ish
    account_label = (
        p.get("account_label")
        or p.get("label")
        or p.get("account_label_slug")
        or ""
    )

    sub_uid_raw = (
        p.get("sub_uid")
        or p.get("subAccountId")
        or p.get("accountId")
        or p.get("subId")
    )

    sub_uid = str(sub_uid_raw) if sub_uid_raw not in (None, "") else ""

    is_main = False
    if not sub_uid:
        # No sub_uid → treat as main unless label explicitly says otherwise
        if str(account_label).lower() in ("", "main", "unified_main", "primary"):
            is_main = True

    if is_main:
        # Hard rule: main gets standard_10 by default
        profile["name"] = "standard_10"
        profile["tp_count"] = 10
        profile["trailing_sl"] = True
        return profile

    # From here on, it's a subaccount
    if strat_mod is None:
        return profile

    if not sub_uid:
        return profile

    # Cleaned-up strategy lookup (no NameError hack)
    try:
        strat = strat_mod.get_strategy_for_sub(str(sub_uid))
    except Exception:
        strat = None

    if not strat:
        return profile

    cfg = strat.get("exit_profile") or strat.get("exitProfile")

    # Helper for mapping profile name -> tp_count / trailing flag
    def _apply_name_only(name_raw: str) -> None:
        name_norm = name_raw.strip().lower()
        if name_norm == "standard_7":
            profile["name"] = "standard_7"
            profile["tp_count"] = 7
            profile["trailing_sl"] = True
        elif name_norm == "standard_5":
            profile["name"] = "standard_5"
            profile["tp_count"] = 5
            profile["trailing_sl"] = True
        elif name_norm == "standard_10":
            profile["name"] = "standard_10"
            profile["tp_count"] = 10
            profile["trailing_sl"] = True
        elif name_norm == "aggressive_7":
            profile["name"] = "aggressive_7"
            profile["tp_count"] = 7
            profile["trailing_sl"] = True
        elif name_norm == "scalp_3":
            profile["name"] = "scalp_3"
            profile["tp_count"] = 3
            profile["trailing_sl"] = True
        else:
            # Unknown name: keep default but store label
            profile["name"] = name_raw

    if isinstance(cfg, dict):
        name = cfg.get("name")
        if isinstance(name, str):
            _apply_name_only(name)

        if "tp_count" in cfg:
            try:
                tp_count_val = int(cfg["tp_count"])
                if tp_count_val > 0:
                    # Hard cap at CORE_TP_COUNT so grid doesn't exceed 10 rungs
                    profile["tp_count"] = min(tp_count_val, CORE_TP_COUNT)
            except Exception:
                pass

        if "trailing_sl" in cfg:
            profile["trailing_sl"] = bool(cfg["trailing_sl"])

    elif isinstance(cfg, str):
        _apply_name_only(cfg)

    return profile


def _safe_tp_price(symbol: str, side_now: str, target_px: Decimal) -> Decimal:
    """
    Enforce a minimum distance between TP price and current market price.
    We do not distort spacing aggressively; we only nudge the whole ladder away
    from the market when needed in _sync_tp_ladder.
    """
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


def _compute_trailing_sl(
    symbol: str,
    side_now: str,
    entry: Decimal,
    base_sl: Decimal,
    tps: List[Decimal],
    trailing_enabled: bool,
) -> Decimal:
    """
    Compute a trailing SL based on best favorable price and R distance.
    """
    if not trailing_enabled or _TRAIL_R_MULT <= 0:
        return base_sl

    try:
        price = Decimal(str(last_price(symbol)))
    except Exception:
        return base_sl

    if price <= 0:
        return base_sl

    if tps:
        R = abs(tps[0] - entry)
    else:
        R = abs(entry - base_sl)

    if R <= 0:
        return base_sl

    trail_dist = R * _TRAIL_R_MULT

    state = _TRAIL_STATE.get(symbol)
    if state is None or state.get("entry") != entry or state.get("base_sl") != base_sl:
        state = {
            "entry": entry,
            "base_sl": base_sl,
            "best": price,
        }
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


def _amend_tp_order(
    symbol: str,
    order: dict,
    new_qty: Optional[Decimal],
    new_price: Optional[Decimal],
    side_now: Optional[str] = None,
) -> None:
    """
    Amend a single TP order in place via REST /v5/order/amend.
    """
    body: Dict[str, str] = {
        "category": CATEGORY,
        "symbol": symbol,
    }
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
    """
    Cancel a single TP order via REST /v5/order/cancel.
    """
    body: Dict[str, str] = {
        "category": CATEGORY,
        "symbol": symbol,
    }
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


def _detect_manual_override(
    symbol: str,
    tpo: List[dict],
    target_tps: List[Decimal],
    core_count: int,
) -> bool:
    """
    Heuristic: if a majority of TP prices deviate from our ideal grid by more than
    ~2 ticks, assume the user manually moved them and enter manual TP mode.
    """
    if not tpo or not target_tps:
        return False

    tick, _step, _ = get_ticks(symbol)
    cur_prices = sorted(Decimal(o["price"]) for o in tpo)
    tgt_sorted = sorted(target_tps)

    n = min(len(cur_prices), len(tgt_sorted), core_count)
    if n == 0:
        return False

    mismatches = 0
    for i in range(n):
        if abs(cur_prices[i] - tgt_sorted[i]) > (tick * 2):
            mismatches += 1

    return mismatches >= 2


def _sync_tp_ladder(
    symbol: str,
    side_now: str,
    size: Decimal,
    tps: List[Decimal],
    tp_count: int,
) -> None:
    """
    Ensure we have a TP ladder (up to CORE_TP_COUNT) with:
      - equal qty per rung
      - evenly spaced grid (tps passed in already equal spacing)
      - NO partial patchy ladders.

    Behaviour:
      - If no existing TPs: build fresh ladder.
      - If manual override detected: keep user prices, only rebalance qty.
      - If auto mode:
          * Amend existing orders toward target grid.
          * Cancel extras.
          * Create missing rungs.
    """
    if tp_count <= 0:
        tp_count = 1
    if tp_count > CORE_TP_COUNT:
        tp_count = CORE_TP_COUNT

    tick, step, _ = get_ticks(symbol)
    target_tps = tps[:tp_count]

    # Base per-rung quantity
    each_default = qdown(size / Decimal(tp_count), step)
    if each_default <= 0:
        # Too small to split properly; fall back to a single mid TP if possible.
        if target_tps:
            mid_idx = min(len(target_tps) - 1, tp_count // 2)
            mid_tp = target_tps[mid_idx]
        else:
            mid_tp = tps[0] if tps else None
        if mid_tp is not None:
            safe_mid = _safe_tp_price(symbol, side_now, mid_tp)
            try:
                place_reduce_tp(symbol, side_now, qdown(size, step), safe_mid)
            except Exception as e:
                alert_bot_error("tp_sl_manager", f"{symbol} single-TP create error: {e}", "WARN")
        return

    orders_all = _open_orders(symbol)
    tpo = _tp_orders(orders_all, side_now)

    # No TPs at all: build a fresh ladder
    if not tpo:
        _MANUAL_TP_MODE.pop(symbol, None)
        # shift entire ladder away from market if needed (but keep spacing)
        tps_sorted = sorted(target_tps)
        shifted = tps_sorted
        if tps_sorted:
            base_safe = _safe_tp_price(symbol, side_now, tps_sorted[0])
            delta = base_safe - tps_sorted[0]
            shifted = [psnap(px + delta, tick) for px in tps_sorted]

        for px in shifted:
            try:
                place_reduce_tp(symbol, side_now, each_default, px)
            except Exception as e:
                alert_bot_error("tp_sl_manager", f"{symbol} TP create error: {e}", "WARN")
        return

    manual_mode = _MANUAL_TP_MODE.get(symbol, False)

    # Manual TP override detection
    if _RESPECT_MANUAL_TPS and not manual_mode:
        if _detect_manual_override(symbol, tpo, target_tps, CORE_TP_COUNT):
            manual_mode = True
            _MANUAL_TP_MODE[symbol] = True
            try:
                send_tg(
                    f"✋ Manual TP override detected for {symbol}. "
                    f"Bot will respect your TP prices until you cancel them or flatten."
                )
            except Exception:
                pass

    # Manual mode: keep prices, only rebalance qty if needed
    if manual_mode and _RESPECT_MANUAL_TPS:
        n = len(tpo)
        if n <= 0:
            _MANUAL_TP_MODE.pop(symbol, None)
            return

        each_manual = qdown(size / Decimal(n), step)
        if each_manual <= 0:
            return

        for o in tpo:
            current_qty = Decimal(o["qty"])
            if current_qty != each_manual:
                _amend_tp_order(
                    symbol,
                    o,
                    new_qty=each_manual,
                    new_price=None,
                    side_now=None,
                )
        return

    # --- Full auto mode (no manual override) below ---
    # Policy: adjust in-place rather than nuking all orders each poll.

    tps_sorted = sorted(target_tps)
    shifted = tps_sorted
    if tps_sorted:
        base_safe = _safe_tp_price(symbol, side_now, tps_sorted[0])
        delta = base_safe - tps_sorted[0]
        shifted = [psnap(px + delta, tick) for px in tps_sorted]

    # Sort existing TP orders by price to align with our grid
    try:
        tpo_sorted = sorted(tpo, key=lambda o: Decimal(str(o.get("price", "0"))))
    except Exception:
        tpo_sorted = tpo

    # Amend existing ones to match our equal ladder
    n_common = min(len(tpo_sorted), len(shifted))
    for i in range(n_common):
        o = tpo_sorted[i]
        target_px = shifted[i]
        _amend_tp_order(
            symbol,
            o,
            new_qty=each_default,
            new_price=target_px,
            side_now=side_now,
        )

    # If there are extra old TPs beyond tp_count, cancel them
    if len(tpo_sorted) > len(shifted):
        for o in tpo_sorted[len(shifted):]:
            _cancel_tp_order(symbol, o)

    # If we need more rungs than we currently have, create the missing ones
    if len(shifted) > len(tpo_sorted):
        for px in shifted[len(tpo_sorted):]:
            try:
                place_reduce_tp(symbol, side_now, each_default, px)
            except Exception as e:
                alert_bot_error("tp_sl_manager", f"{symbol} TP create (extra rung) error: {e}", "WARN")


def _extract_existing_sl(p: dict) -> Optional[Decimal]:
    """
    Best-effort extraction of the current stop-loss price from a position dict.
    """
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


def _ensure_exits_for_position(
    p: dict,
    seen_state: Dict[str, Tuple[Decimal, Decimal]],
) -> None:
    """
    For a single position record, ensure SL + TP ladder exist and are balanced with size.
    """
    symbol = p["symbol"]
    side_now = p["side"]  # "Buy"/"Sell"
    entry = Decimal(str(p["avgPrice"]))

    size = Decimal(str(p["size"]))

    if size <= 0:
        seen_state.pop(symbol, None)
        _MANUAL_TP_MODE.pop(symbol, None)
        _MANUAL_SL_MODE.pop(symbol, None)
        _TRAIL_STATE.pop(symbol, None)
        return

    prev_state = seen_state.get(symbol)
    state = (entry, size)

    exit_profile = _get_exit_profile_for_position(p)
    tp_count = int(exit_profile.get("tp_count", CORE_TP_COUNT) or CORE_TP_COUNT)
    trailing_sl = bool(exit_profile.get("trailing_sl", True))

    base_sl, tps_full = _compute_exit_grid(symbol, side_now, entry, tp_count)

    tick, _step, _ = get_ticks(symbol)
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

    if manual_sl_mode and existing_sl is not None:
        sl_effective = existing_sl
    else:
        sl_effective = _compute_trailing_sl(
            symbol=symbol,
            side_now=side_now,
            entry=entry,
            base_sl=base_sl,
            tps=tps_full,
            trailing_enabled=trailing_sl,
        )
        set_stop_loss(symbol, sl_effective)

    # Always ensure ladder shape matches current size/profile
    _sync_tp_ladder(symbol, side_now, size, tps_full, tp_count=tp_count)

    if prev_state != state:
        used_tps = tps_full[:tp_count]
        try:
            send_tg(
                f"🎯 Exits set {symbol} {side_now} | size {size} | "
                f"profile {exit_profile.get('name')} | "
                f"SL {sl_effective} | TPs {', '.join(map(str, used_tps))}"
            )
        except Exception:
            pass

    seen_state[symbol] = state


# ---------------------------------------------------------------------------
# HTTP polling mode (position-bus powered)
# ---------------------------------------------------------------------------

def _loop_http_poll() -> None:
    """
    Polls positions every POLL_SECONDS and ensures exits are attached.

    HTTP mode now uses position_bus.get_positions_snapshot(), which:
      - Reads state/positions_bus.json if fresh enough.
      - Falls back to REST list_open_positions(category="linear") for MAIN
        when snapshot is missing/stale, and updates positions_bus.json.
    """
    label = os.getenv("ACCOUNT_LABEL", "main")
    # Console + Telegram so you *know* it's online
    print(f"[tp_sl_manager] ONLINE in HTTP + position_bus mode | label={label} | poll={POLL_SECONDS}s")
    try:
        send_tg(
            f"🎛 Flashback TP/SL Manager ONLINE (HTTP + position_bus, label={label}, {POLL_SECONDS}s)."
        )
    except Exception:
        pass

    seen: Dict[str, Tuple[Decimal, Decimal]] = {}

    while True:
        record_heartbeat("tp_sl_manager")
        try:
            # label=None means "use ACCOUNT_LABEL" inside position_bus
            positions = bus_get_positions_snapshot(
                label=None,
                category=CATEGORY,
                max_age_seconds=None,      # let position_bus decide age or use its default
                allow_rest_fallback=True,
            )
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

            time.sleep(POLL_SECONDS)
        except Exception as e:
            alert_bot_error("tp_sl_manager", f"HTTP loop error: {e}", "ERROR")
            time.sleep(5)


# ---------------------------------------------------------------------------
# WebSocket mode: private stream for positions (direct WS)
# ---------------------------------------------------------------------------

def _handle_ws_position_message(
    msg: dict,
    seen: Dict[str, Tuple[Decimal, Decimal]],
) -> None:
    """
    Handle a Bybit private 'position' topic push.
    """
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
            continue

        norm = {
            "symbol": symbol,
            "side": p.get("side"),
            "avgPrice": p.get("avgPrice"),
            "size": p.get("size"),
            "stopLoss": p.get("stopLoss") or p.get("stopLossPrice") or p.get("slPrice"),
            "sub_uid": p.get("sub_uid") or p.get("subAccountId") or p.get("accountId") or p.get("subId"),
        }
        _ensure_exits_for_position(norm, seen_state=seen)

    for s in list(seen.keys()):
        if s not in current_symbols:
            seen.pop(s, None)
            _MANUAL_TP_MODE.pop(s, None)
            _MANUAL_SL_MODE.pop(s, None)
            _TRAIL_STATE.pop(s, None)


def _loop_ws() -> None:
    """
    WebSocket-only main loop:
    - Connects to BYBIT_WS_PRIVATE_URL
    - Authenticates using shared auth builder
    - Subscribes to "position" private topic
    """
    if websocket is None:
        raise RuntimeError("websocket-client is not installed. pip install websocket-client")

    label = os.getenv("ACCOUNT_LABEL", "main")
    print(f"[tp_sl_manager] ONLINE in WebSocket mode | label={label}")
    try:
        send_tg(f"🎛 Flashback TP/SL Manager ONLINE (WebSocket mode, label={label}).")
    except Exception:
        pass

    seen: Dict[str, Tuple[Decimal, Decimal]] = {}

    while True:
        ws = None
        try:
            ws = websocket.create_connection(BYBIT_WS_PRIVATE_URL, timeout=5)

            # Auth using shared helper (correct Bybit v5 format)
            auth_msg = build_ws_auth_payload_main()
            ws.send(json.dumps(auth_msg))

            raw = ws.recv()
            resp = json.loads(raw)
            if resp.get("success") is False or resp.get("retCode", 0) != 0:
                raise RuntimeError(f"WS auth failed: {resp}")

            sub = {"op": "subscribe", "args": ["position"]}
            ws.send(json.dumps(sub))

            last_ping = time.time()

            while True:
                record_heartbeat("tp_sl_manager")

                now = time.time()
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


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def loop():
    """
    Entry point called by supervisor.
    Chooses WebSocket mode or HTTP poll mode depending on TPM_USE_WEBSOCKET.

    - HTTP mode: uses position_bus.get_positions_snapshot() (WS-fed file + REST fallback).
    - WS mode  : uses direct private WS and ignores position_bus.
    """
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
