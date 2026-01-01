#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Main Mover Breakout Bot (SIMPLE)

Goal:
- Scan Bybit USDT linear perpetuals
- Focus "low-cap style movers" via proxies:
    • max leverage >= MIN_LEVERAGE
    • 24h turnover >= MIN_TURNOVER_USDT
    • spread% <= MAX_SPREAD_PCT
    • ATR%(15m) >= MIN_ATR_PCT_15M
- Signal (simple + robust-ish):
    • 1h trend filter: last close > EMA50 and EMA50 rising
    • 5m trigger: close breaks Donchian(20) high/low
    • 5m confirmation: Volume Z-score >= VOL_Z_MIN
- Execute:
    • one position at a time
    • cross margin
    • set max leverage
    • market entry
    • place SL stop-market (conditional reduce-only)
    • place 7 reduce-only TP limit orders

Notes:
- Uses your thin wrapper: app.core.bybit_client.Bybit
- Assumes bybit_get/bybit_post already handle auth + UTA subaccount routing
- Designed to be "simple logic, not suicidal mechanics"
"""

from __future__ import annotations

import json
import math
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from app.core.bybit_client import Bybit


# -----------------------------
# Config (keep it boring)
# -----------------------------

CATEGORY = "linear"          # USDT perpetuals
QUOTE = "USDT"

SCAN_EVERY_SEC = 45
TOP_N = 12                  # only compute klines for top N candidates each scan (rate limits)

MIN_LEVERAGE = 20           # your "low cap alt" proxy: allow high leverage instruments
MIN_TURNOVER_USDT = 5_000_000   # 24h turnover floor (tune; too low = garbage fills)
MAX_SPREAD_PCT = 0.25       # 0.25% spread max (tune; too strict = no trades)

MIN_ATR_PCT_15M = 0.9       # require real movement
ADX_MIN_15M = 18.0          # anti-chop (still simple)

DONCHIAN_N_5M = 20
EMA_N_1H = 50
VOL_Z_N_5M = 20
VOL_Z_MIN = 2.0

# Risk model (simple)
RISK_PCT = 0.004            # 0.4% of equity per trade (cross + max leverage needs discipline)
SL_ATR_MULT = 1.2           # SL distance = 1.2 * ATR(5m)

# 7 TP ladder in R-multiples (R = SL distance)
TP_R_LEVELS = [0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 4.0]
TP_WEIGHTS = [0.10, 0.15, 0.15, 0.15, 0.15, 0.15, 0.15]  # must sum to 1.0

# Reduce-only TPs: Limit orders
TP_POST_ONLY = False        # True if you want to be maker-only (may miss)
TIME_IN_FORCE = "GTC"

# Logging
STATE_DIR = Path("state")
LOG_DIR = STATE_DIR / "bots"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_PATH = LOG_DIR / "main_mover_breakout_bot.jsonl"

# Cooldowns
LOSS_COOLDOWN_SEC = 15 * 60
MAX_LOSSES_PER_DAY = 2


# -----------------------------
# Helpers: math/indicators
# -----------------------------

def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def ema(values: List[float], n: int) -> Optional[float]:
    if len(values) < n or n <= 1:
        return None
    k = 2.0 / (n + 1.0)
    e = values[0]
    for v in values[1:]:
        e = (v * k) + (e * (1.0 - k))
    return e


def atr(high: List[float], low: List[float], close: List[float], n: int) -> Optional[float]:
    if len(close) < n + 1:
        return None
    trs: List[float] = []
    for i in range(1, len(close)):
        tr = max(
            high[i] - low[i],
            abs(high[i] - close[i - 1]),
            abs(low[i] - close[i - 1]),
        )
        trs.append(tr)
    if len(trs) < n:
        return None
    # simple moving average ATR (keeps it simple)
    return sum(trs[-n:]) / float(n)


def donchian(high: List[float], low: List[float], n: int) -> Tuple[Optional[float], Optional[float]]:
    if len(high) < n or len(low) < n:
        return None, None
    return max(high[-n:]), min(low[-n:])


def zscore(values: List[float]) -> Optional[float]:
    if len(values) < 2:
        return None
    m = sum(values) / float(len(values))
    var = sum((v - m) ** 2 for v in values) / float(len(values) - 1)
    sd = math.sqrt(var) if var > 0 else 0.0
    if sd == 0.0:
        return 0.0
    return (values[-1] - m) / sd


def adx(high: List[float], low: List[float], close: List[float], n: int = 14) -> Optional[float]:
    """
    Minimal ADX implementation (Wilder smoothing) to avoid chop.
    Not fancy, but good enough for filtering.
    """
    if len(close) < n + 2:
        return None

    # directional movement & true range
    tr_list = []
    plus_dm = []
    minus_dm = []

    for i in range(1, len(close)):
        up_move = high[i] - high[i - 1]
        down_move = low[i - 1] - low[i]
        p_dm = up_move if (up_move > down_move and up_move > 0) else 0.0
        m_dm = down_move if (down_move > up_move and down_move > 0) else 0.0

        tr = max(
            high[i] - low[i],
            abs(high[i] - close[i - 1]),
            abs(low[i] - close[i - 1]),
        )

        tr_list.append(tr)
        plus_dm.append(p_dm)
        minus_dm.append(m_dm)

    if len(tr_list) < n:
        return None

    # Wilder smoothing (start with sums)
    tr14 = sum(tr_list[:n])
    p14 = sum(plus_dm[:n])
    m14 = sum(minus_dm[:n])

    dxs: List[float] = []
    for i in range(n, len(tr_list)):
        # smooth
        tr14 = tr14 - (tr14 / n) + tr_list[i]
        p14 = p14 - (p14 / n) + plus_dm[i]
        m14 = m14 - (m14 / n) + minus_dm[i]

        if tr14 <= 0:
            continue

        pdi = 100.0 * (p14 / tr14)
        mdi = 100.0 * (m14 / tr14)
        denom = (pdi + mdi)
        dx = 0.0 if denom == 0 else (100.0 * abs(pdi - mdi) / denom)
        dxs.append(dx)

    if len(dxs) < n:
        return None

    # ADX is Wilder-smoothed DX
    adx_val = sum(dxs[:n]) / float(n)
    for i in range(n, len(dxs)):
        adx_val = ((adx_val * (n - 1)) + dxs[i]) / float(n)
    return adx_val


# -----------------------------
# Bybit REST wrappers
# -----------------------------

@dataclass
class Ticker:
    symbol: str
    last_price: float
    bid1: float
    ask1: float
    turnover_24h: float
    funding_rate: float


def log_event(event: Dict[str, Any]) -> None:
    event["ts"] = int(time.time() * 1000)
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(event, ensure_ascii=False) + "\n")


def bybit_ok(resp: Dict[str, Any]) -> bool:
    # Bybit often uses retCode == 0
    return _safe_float(resp.get("retCode"), -1) == 0


def get_instruments(client: Bybit) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    cursor: Optional[str] = None

    # Instruments endpoint supports pagination with cursor.
    # We'll loop a few pages defensively.
    for _ in range(10):
        params: Dict[str, Any] = {"category": CATEGORY}
        if cursor:
            params["cursor"] = cursor
        resp = client.get("/v5/market/instruments-info", params)
        if not bybit_ok(resp):
            log_event({"type": "error", "where": "get_instruments", "resp": resp})
            break
        result = resp.get("result") or {}
        items = result.get("list") or []
        out.extend(items)
        cursor = result.get("nextPageCursor")
        if not cursor:
            break
    return out


def get_tickers(client: Bybit) -> List[Ticker]:
    resp = client.get("/v5/market/tickers", {"category": CATEGORY})
    if not bybit_ok(resp):
        log_event({"type": "error", "where": "get_tickers", "resp": resp})
        return []
    items = (resp.get("result") or {}).get("list") or []
    out: List[Ticker] = []
    for it in items:
        symbol = str(it.get("symbol", ""))
        lastp = _safe_float(it.get("lastPrice"))
        bid1 = _safe_float(it.get("bid1Price"))
        ask1 = _safe_float(it.get("ask1Price"))
        turnover = _safe_float(it.get("turnover24h"))
        funding = _safe_float(it.get("fundingRate"))
        out.append(Ticker(symbol=symbol, last_price=lastp, bid1=bid1, ask1=ask1,
                          turnover_24h=turnover, funding_rate=funding))
    return out


def get_kline(client: Bybit, symbol: str, interval: str, limit: int) -> Optional[List[List[Any]]]:
    # /v5/market/kline returns list of arrays: [start, open, high, low, close, volume, turnover]
    resp = client.get("/v5/market/kline", {"category": CATEGORY, "symbol": symbol, "interval": interval, "limit": limit})
    if not bybit_ok(resp):
        log_event({"type": "error", "where": "get_kline", "symbol": symbol, "interval": interval, "resp": resp})
        return None
    data = (resp.get("result") or {}).get("list") or []
    # typically reverse-chronological; sort oldest->newest
    try:
        data_sorted = sorted(data, key=lambda r: int(r[0]))
        return data_sorted
    except Exception:
        return data


def get_positions(client: Bybit) -> List[Dict[str, Any]]:
    resp = client.get("/v5/position/list", {"category": CATEGORY})
    if not bybit_ok(resp):
        log_event({"type": "error", "where": "get_positions", "resp": resp})
        return []
    return (resp.get("result") or {}).get("list") or []


def get_wallet_balance(client: Bybit) -> float:
    # Best effort: UTA wallet-balance endpoint is /v5/account/wallet-balance
    resp = client.get("/v5/account/wallet-balance", {"accountType": "UNIFIED"})
    if not bybit_ok(resp):
        log_event({"type": "error", "where": "get_wallet_balance", "resp": resp})
        return 0.0
    lst = (resp.get("result") or {}).get("list") or []
    if not lst:
        return 0.0
    # Find USDT equity-ish value; fields vary. We'll try totalEquity, then totalWalletBalance.
    first = lst[0] or {}
    eq = _safe_float(first.get("totalEquity"))
    if eq > 0:
        return eq
    return _safe_float(first.get("totalWalletBalance"))


def set_cross_margin(client: Bybit, symbol: str) -> bool:
    # Switch Cross/Isolated Margin: tradeMode 0=cross, 1=isolated :contentReference[oaicite:4]{index=4}
    body = {"category": CATEGORY, "symbol": symbol, "tradeMode": 0}
    resp = client.post("/v5/position/switch-isolated", body)
    ok = bybit_ok(resp)
    if not ok:
        log_event({"type": "error", "where": "set_cross_margin", "symbol": symbol, "resp": resp})
    return ok


def set_leverage_max(client: Bybit, symbol: str, lev: int) -> bool:
    # Set leverage: POST /v5/position/set-leverage :contentReference[oaicite:5]{index=5}
    body = {"category": CATEGORY, "symbol": symbol, "buyLeverage": str(lev), "sellLeverage": str(lev)}
    resp = client.post("/v5/position/set-leverage", body)
    ok = bybit_ok(resp)
    if not ok:
        log_event({"type": "error", "where": "set_leverage", "symbol": symbol, "lev": lev, "resp": resp})
    return ok


# -----------------------------
# Strategy
# -----------------------------

@dataclass
class Candidate:
    symbol: str
    score: float
    last_price: float
    spread_pct: float
    turnover_24h: float
    max_leverage: int


@dataclass
class Signal:
    symbol: str
    side: str               # "Buy" or "Sell"
    entry_price: float
    sl_price: float
    r_dist: float


def compute_spread_pct(bid: float, ask: float) -> float:
    if bid <= 0 or ask <= 0:
        return 999.0
    mid = (bid + ask) / 2.0
    if mid <= 0:
        return 999.0
    return 100.0 * ((ask - bid) / mid)


def build_universe(client: Bybit) -> Dict[str, int]:
    """
    Map symbol -> maxLeverage (int), for USDT linear perps.
    """
    instruments = get_instruments(client)
    maxlev: Dict[str, int] = {}
    for it in instruments:
        sym = str(it.get("symbol", ""))
        # Keep USDT quote only (avoid weird stuff)
        if not sym.endswith(QUOTE):
            continue
        # Some instrument entries store leverage in leverageFilter
        levf = it.get("leverageFilter") or {}
        maxL = int(_safe_float(levf.get("maxLeverage"), 0))
        if maxL <= 0:
            continue
        maxlev[sym] = maxL
    return maxlev


def rank_candidates(client: Bybit, maxlev_map: Dict[str, int]) -> List[Candidate]:
    tickers = get_tickers(client)
    cands: List[Candidate] = []

    for t in tickers:
        if t.symbol not in maxlev_map:
            continue
        maxL = maxlev_map[t.symbol]
        if maxL < MIN_LEVERAGE:
            continue
        if t.turnover_24h < MIN_TURNOVER_USDT:
            continue

        spread_pct = compute_spread_pct(t.bid1, t.ask1)
        if spread_pct > MAX_SPREAD_PCT:
            continue

        # Basic score: reward volatility + liquidity, punish spread
        # We'll add ATR% later when we compute klines for TOP_N.
        base_score = (math.log10(max(t.turnover_24h, 1.0)) * 10.0) / max(spread_pct, 0.05)
        cands.append(Candidate(
            symbol=t.symbol,
            score=base_score,
            last_price=t.last_price,
            spread_pct=spread_pct,
            turnover_24h=t.turnover_24h,
            max_leverage=maxL
        ))

    cands.sort(key=lambda x: x.score, reverse=True)
    return cands


def compute_signal(client: Bybit, symbol: str, last_price: float) -> Optional[Signal]:
    # Fetch klines (limit small to be efficient)
    k1h = get_kline(client, symbol, "60", 120)
    k15 = get_kline(client, symbol, "15", 120)
    k5 = get_kline(client, symbol, "5", 120)
    if not k1h or not k15 or not k5:
        return None

    def _col(k: List[List[Any]], idx: int) -> List[float]:
        return [_safe_float(r[idx]) for r in k]

    o1, h1, l1, c1, v1 = _col(k1h, 1), _col(k1h, 2), _col(k1h, 3), _col(k1h, 4), _col(k1h, 5)
    o15, h15, l15, c15, v15 = _col(k15, 1), _col(k15, 2), _col(k15, 3), _col(k15, 4), _col(k15, 5)
    o5, h5, l5, c5, v5 = _col(k5, 1), _col(k5, 2), _col(k5, 3), _col(k5, 4), _col(k5, 5)

    # Trend filter (1h)
    ema50 = ema(c1[-EMA_N_1H:], EMA_N_1H)
    if ema50 is None:
        return None
    # slope: compare now to 5 bars ago
    ema50_prev = ema(c1[-(EMA_N_1H + 5):-5], EMA_N_1H) if len(c1) >= EMA_N_1H + 5 else None
    if ema50_prev is None:
        return None

    trend_up = (c1[-1] > ema50) and (ema50 > ema50_prev)
    trend_dn = (c1[-1] < ema50) and (ema50 < ema50_prev)

    # Anti-chop (15m ADX)
    adx15 = adx(h15, l15, c15, 14)
    if adx15 is None or adx15 < ADX_MIN_15M:
        return None

    # Volatility requirement (15m ATR%)
    atr15 = atr(h15, l15, c15, 14)
    if atr15 is None:
        return None
    atr_pct_15m = 100.0 * (atr15 / max(c15[-1], 1e-9))
    if atr_pct_15m < MIN_ATR_PCT_15M:
        return None

    # Trigger (5m Donchian break + volume z-score)
    dc_hi, dc_lo = donchian(h5[:-1], l5[:-1], DONCHIAN_N_5M)  # donchian on prior bars
    if dc_hi is None or dc_lo is None:
        return None
    close_now = c5[-1]
    volz = zscore(v5[-VOL_Z_N_5M:])
    if volz is None or volz < VOL_Z_MIN:
        return None

    # SL distance based on 5m ATR
    atr5 = atr(h5, l5, c5, 14)
    if atr5 is None:
        return None
    r_dist = max(atr5 * SL_ATR_MULT, close_now * 0.002)  # also enforce a minimum ~0.2%

    # LONG
    if trend_up and close_now > dc_hi:
        sl = close_now - r_dist
        return Signal(symbol=symbol, side="Buy", entry_price=close_now, sl_price=sl, r_dist=r_dist)

    # SHORT
    if trend_dn and close_now < dc_lo:
        sl = close_now + r_dist
        return Signal(symbol=symbol, side="Sell", entry_price=close_now, sl_price=sl, r_dist=r_dist)

    return None


# -----------------------------
# Execution (orders)
# -----------------------------

def has_open_position(positions: List[Dict[str, Any]]) -> bool:
    for p in positions:
        sym = str(p.get("symbol", ""))
        size = _safe_float(p.get("size"))
        if sym and size != 0.0:
            return True
    return False


def position_for_symbol(positions: List[Dict[str, Any]], symbol: str) -> Optional[Dict[str, Any]]:
    for p in positions:
        if str(p.get("symbol", "")) == symbol and _safe_float(p.get("size")) != 0.0:
            return p
    return None


def compute_qty_from_risk(equity: float, entry: float, sl: float) -> float:
    """
    Qty sized by risk in quote terms:
    risk_usdt = equity * RISK_PCT
    stop_dist = abs(entry - sl)
    qty ~= risk_usdt / stop_dist
    """
    risk_usdt = max(equity * RISK_PCT, 1.0)
    stop_dist = max(abs(entry - sl), entry * 0.0005)
    qty = risk_usdt / stop_dist
    return max(qty, 0.0)


def place_entry_and_exits(client: Bybit, sig: Signal, max_leverage: int) -> bool:
    symbol = sig.symbol
    side = sig.side
    entry = sig.entry_price
    sl = sig.sl_price
    r = sig.r_dist

    # Ensure cross + leverage
    set_cross_margin(client, symbol)
    set_leverage_max(client, symbol, max_leverage)

    equity = get_wallet_balance(client)
    qty = compute_qty_from_risk(equity, entry, sl)

    # Market entry
    entry_resp = client.place_order(
        category=CATEGORY,
        symbol=symbol,
        side=side,
        qty=qty,
        orderType="Market",
    )
    if not bybit_ok(entry_resp):
        log_event({"type": "error", "where": "entry_order", "symbol": symbol, "resp": entry_resp})
        return False

    log_event({
        "type": "entry",
        "symbol": symbol,
        "side": side,
        "qty": qty,
        "entry_ref": entry_resp,
        "entry_price_est": entry,
        "sl_price": sl,
        "r_dist": r,
        "max_leverage": max_leverage,
    })

    # Determine exit side
    exit_side = "Sell" if side == "Buy" else "Buy"

    # Stop-market (conditional). Keep it best-effort.
    # Bybit v5 order create supports stop/trigger fields, but exact behavior depends on market rules.
    # We'll send common fields; if rejected, you'll see it in logs and we’ll patch fast.
    trigger_direction = 2 if side == "Buy" else 1  # 2=price falls triggers (long SL), 1=price rises triggers (short SL)

    sl_resp = client.place_order(
        category=CATEGORY,
        symbol=symbol,
        side=exit_side,
        qty=qty,
        orderType="Market",
        reduceOnly=True,
        closeOnTrigger=True,
        triggerPrice=str(sl),
        triggerDirection=trigger_direction,
        triggerBy="LastPrice",
    )
    if not bybit_ok(sl_resp):
        log_event({"type": "error", "where": "stop_loss", "symbol": symbol, "resp": sl_resp})
    else:
        log_event({"type": "sl_placed", "symbol": symbol, "sl": sl, "resp": sl_resp})

    # 7 TP reduce-only limit orders
    for i, (lvl, w) in enumerate(zip(TP_R_LEVELS, TP_WEIGHTS), start=1):
        tp_price = (entry + (lvl * r)) if side == "Buy" else (entry - (lvl * r))
        tp_qty = max(qty * w, 0.0)

        tp_body: Dict[str, Any] = dict(
            category=CATEGORY,
            symbol=symbol,
            side=exit_side,
            qty=tp_qty,
            orderType="Limit",
            price=str(tp_price),
            reduceOnly=True,
            timeInForce=TIME_IN_FORCE,
        )
        if TP_POST_ONLY:
            tp_body["postOnly"] = True

        tp_resp = client.post("/v5/order/create", tp_body)
        if not bybit_ok(tp_resp):
            log_event({"type": "error", "where": f"tp{i}", "symbol": symbol, "tp_price": tp_price, "resp": tp_resp})
        else:
            log_event({"type": "tp_placed", "symbol": symbol, "tp_n": i, "tp_price": tp_price, "tp_qty": tp_qty, "resp": tp_resp})

    return True


# -----------------------------
# Main loop
# -----------------------------

def main() -> None:
    client = Bybit(key_role="trade")
    log_event({"type": "boot", "msg": "main_mover_breakout_bot starting"})

    maxlev_map = build_universe(client)
    log_event({"type": "universe", "count": len(maxlev_map)})

    losses_today = 0
    day_key = time.strftime("%Y-%m-%d")
    last_loss_ts = 0.0

    while True:
        try:
            # Reset daily counters
            now_day = time.strftime("%Y-%m-%d")
            if now_day != day_key:
                day_key = now_day
                losses_today = 0

            # Position gate
            positions = get_positions(client)
            if has_open_position(positions):
                # We do nothing while in position (TP/SL orders manage exits)
                log_event({"type": "status", "msg": "position_open_hold"})
                time.sleep(SCAN_EVERY_SEC)
                continue

            # Cooldown gate
            if losses_today >= MAX_LOSSES_PER_DAY:
                log_event({"type": "guard", "msg": "max_losses_hit", "losses_today": losses_today})
                time.sleep(SCAN_EVERY_SEC)
                continue
            if last_loss_ts and (time.time() - last_loss_ts) < LOSS_COOLDOWN_SEC:
                log_event({"type": "guard", "msg": "loss_cooldown"})
                time.sleep(SCAN_EVERY_SEC)
                continue

            # Rank candidates from tickers/instruments
            cands = rank_candidates(client, maxlev_map)
            shortlist = cands[:TOP_N]
            log_event({"type": "scan", "shortlist": [c.symbol for c in shortlist]})

            # Compute signals for shortlist only
            chosen: Optional[Signal] = None
            chosen_maxL: int = 0
            for c in shortlist:
                sig = compute_signal(client, c.symbol, c.last_price)
                if sig:
                    chosen = sig
                    chosen_maxL = c.max_leverage
                    break

            if not chosen:
                time.sleep(SCAN_EVERY_SEC)
                continue

            log_event({"type": "signal", "symbol": chosen.symbol, "side": chosen.side, "entry_est": chosen.entry_price})

            ok = place_entry_and_exits(client, chosen, chosen_maxL)

            if not ok:
                # treat as a "loss-like" event to avoid rapid-fire failures
                last_loss_ts = time.time()
                losses_today += 1

            time.sleep(SCAN_EVERY_SEC)

        except KeyboardInterrupt:
            log_event({"type": "stop", "msg": "KeyboardInterrupt"})
            break
        except Exception as e:
            log_event({"type": "exception", "err": repr(e)})
            time.sleep(10)


if __name__ == "__main__":
    main()
