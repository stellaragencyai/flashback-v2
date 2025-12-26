#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback / Base44 — Signal Engine v2 (Strategy-aware + AI-logging + JSONL export)

What this does:
- If SIG_USE_STRATEGIES=true (default) and app.core.strategies is available:
    • Reads config/strategies.yaml via app.core.strategies.
    • Builds a universe:
          (symbol, timeframe) -> list of strategy dicts (per subaccount).
    • Only scans symbols/TFs belonging to at least one sub-strategy.
    • Emits signals tagged with strategy name + sub_uid for each matched strategy.
- Otherwise (fallback mode):
    • Uses SIG_SYMBOLS / SIG_TIMEFRAMES from .env (like v1, generic).

Shared behavior:
- Uses Bybit public kline endpoint to fetch recent candles.
- For each (symbol, timeframe), decides a simple LONG/SHORT bias:
    • LONG  if last close > previous close AND last close > simple MA(last N closes)
    • SHORT if last close < previous close AND last close < simple MA(last N closes)
    • Otherwise: no signal.
- Emits at most ONE signal per closed bar per (symbol, timeframe).
- Sends human-readable alerts via the main Telegram notifier.
- Logs every signal into the AI event store via app.core.ai_hooks.log_signal_from_engine.
- ALSO appends machine-readable signals to signals/observed.jsonl for auto_executor.

.env keys used (all optional with defaults):
    BYBIT_BASE                = https://api.bybit.com

    SIG_ENABLED               = true
    SIG_DRY_RUN               = true           # currently unused (no orders placed)
    SIG_SYMBOLS               = BTCUSDT,ETHUSDT        # fallback-only
    SIG_TIMEFRAMES            = 5,15                   # fallback-only
    SIG_POLL_SEC              = 15
    SIG_HEARTBEAT_SEC         = 300
    SIG_USE_STRATEGIES        = true          # use app.core.strategies universe

Telegram:
    Uses app.core.notifier_bot.get_notifier("main")

Run from project root:
    python -m app.bots.signal_engine
"""

from __future__ import annotations

import os
import time
import json as _jsonlib
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional, Iterable

# ---------- HTTP client ----------

try:
    import requests  # type: ignore

    _HAS_REQUESTS = True
except Exception:
    # Fallback to stdlib if `requests` is not installed
    import urllib.request as _urllib_request  # type: ignore
    import urllib.parse as _urllib_parse      # type: ignore
    import json as _json_fallback

    _HAS_REQUESTS = False

from dotenv import load_dotenv

# -------------------------------
# Strategy Setup Logic Dispatch
# -------------------------------

from typing import Callable
from statistics import mean, stdev


def compute_regime_indicators(candles: List[Dict[str, Any]]) -> dict:
    """
    Computes basic regime indicators for a list of candles (oldest->newest):
      - ADX (simple approx trend strength)
      - ATR % (volatility relative to close)
      - Volume z-score (relative volume)
    """

    trs = []
    closes = [c["close"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]

    for i in range(1, len(candles)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
        trs.append(tr)

    if trs:
        atr = mean(trs[-14:])  # approx 14-period ATR
    else:
        atr = 0.0

    last_close = closes[-1]
    atr_pct = (atr / last_close * 100) if last_close else 0.0

    vols = [c["volume"] for c in candles]
    if len(vols) >= 10:
        mu = mean(vols)
        sigma = stdev(vols) if len(vols) > 1 else 1.0
        vol_z = (vols[-1] - mu) / sigma if sigma != 0 else 0.0
    else:
        vol_z = 0.0

    if len(closes) >= 2:
        diffs = [abs(closes[i] - closes[i - 1]) for i in range(1, len(closes))]
        adx = mean(diffs[-14:]) / last_close * 100 if last_close else 0.0
    else:
        adx = 0.0

    return {
        "adx": adx,
        "atr_pct": atr_pct,
        "vol_z": vol_z,
    }


def _get_strat_attr(obj, key: str, default=None):
    """
    Robust attribute extractor for strategy config.
    Supports:
      - dict objects
      - pydantic/dataclass-style objects via getattr
      - Strategy objects that store the real payload in obj.raw (dict)
      - model_dump()/dict() fallbacks
    """
    # REPLACED_GET_STRAT_ATTR_V2
    if obj is None:
        return default

    # 1) dict
    if isinstance(obj, dict):
        v = obj.get(key, default)
        return default if v is None else v

    # 2) direct attribute
    try:
        if hasattr(obj, key):
            v = getattr(obj, key)
            if v is not None:
                return v
    except Exception:
        pass

    # 3) unwrap obj.raw dict (your Strategy case)
    try:
        raw = getattr(obj, "raw", None)
        if isinstance(raw, dict):
            v = raw.get(key, None)
            if v is not None:
                return v
    except Exception:
        pass

    # 4) model_dump()/dict() fallback
    try:
        if hasattr(obj, "model_dump"):
            d = obj.model_dump()
            if isinstance(d, dict):
                v = d.get(key, None)
                if v is not None:
                    return v
        if hasattr(obj, "dict"):
            d = obj.dict()
            if isinstance(d, dict):
                v = d.get(key, None)
                if v is not None:
                    return v
    except Exception:
        pass

    return default


def passes_regime_filters(strategy_raw: Any, regime_ind: dict) -> bool:
    """
    Checks the regime_ind values against the strategy's regime filters
    if they are defined in the strategy raw (dict OR object).
    """

    regime_cfg = _get_strat_attr(strategy_raw, "regime", None) or {}
    if not isinstance(regime_cfg, dict):
        # if someone made it an object, we ignore rather than crash
        regime_cfg = {}

    min_adx = float(regime_cfg.get("min_adx", 0.0) or 0.0)
    max_atr = float(regime_cfg.get("max_atr_pct", float("inf")) or float("inf"))
    min_vol_z = float(regime_cfg.get("min_vol_z", -float("inf")) or -float("inf"))
    max_vol_z = float(regime_cfg.get("max_vol_z", float("inf")) or float("inf"))

    adx_val = float(regime_ind.get("adx", 0.0) or 0.0)
    atr_val = float(regime_ind.get("atr_pct", 0.0) or 0.0)
    vol_val = float(regime_ind.get("vol_z", 0.0) or 0.0)

    if adx_val < min_adx:
        return False
    if atr_val > max_atr:
        return False
    if vol_val < min_vol_z:
        return False
    if vol_val > max_vol_z:
        return False

    return True


# Each function returns (side: "LONG"/"SHORT"/None, reason: str)

def signal_ma_trend(candles: List[dict]) -> Tuple[Optional[str], str]:
    closes = [c["close"] for c in candles]
    ma = sum(closes[-8:]) / min(len(closes[-8:]), 8)
    last = candles[-1]["close"]
    prev = candles[-2]["close"]
    if last > prev and last > ma:
        return "LONG", "trend_ma"
    if last < prev and last < ma:
        return "SHORT", "trend_ma"
    return None, "trend_ma_none"


def signal_breakout(candles: List[dict]) -> Tuple[Optional[str], str]:
    highs = [c["high"] for c in candles[:-1]]
    lows = [c["low"] for c in candles[:-1]]
    last_close = candles[-1]["close"]
    if highs and last_close > max(highs):
        return "LONG", "breakout_high"
    if lows and last_close < min(lows):
        return "SHORT", "breakout_low"
    return None, "breakout_none"


SETUP_LOGIC: Dict[str, Callable[[List[dict]], Tuple[Optional[str], str]]] = {
    "trend_pullback": signal_ma_trend,
    "trend_breakout_retest": signal_breakout,
    "ema_trend_follow": signal_ma_trend,

    "breakout_high": signal_breakout,
    "breakout_range": signal_breakout,
    "squeeze_release": signal_breakout,

    "scalp_liquidity_sweep": signal_ma_trend,
    "scalp_trend_continuation": signal_ma_trend,
    "scalp_reversal_snapback": signal_breakout,

    "swing_reversion_extreme": signal_breakout,
    "swing_reversion_channel": signal_breakout,

    "mm_spread_capture": signal_ma_trend,
    "mm_reversion_micro": signal_ma_trend,

    "pump_chase_momo": signal_ma_trend,
    "dump_fade_reversion": signal_ma_trend,

    "intraday_range_fade": signal_breakout,
    "failed_breakout_fade": signal_breakout,
}

# ---------- Telegram notifier ----------
from app.core.notifier_bot import get_notifier

# ---------- AI logging (optional) ----------
try:
    from app.core.ai_hooks import log_signal_from_engine as _real_log_signal_from_engine  # type: ignore
    _HAS_AI_HOOKS = True
except Exception:
    _HAS_AI_HOOKS = False

    def _real_log_signal_from_engine(
        *,
        symbol: str,
        timeframe: str,
        side: str,
        source: str,
        confidence: Optional[float],
        stop_hint: Optional[float],
        owner: str,
        sub_uid: Optional[str],
        strategy_role: str,
        regime_tags: List[str],
        extra: Dict[str, Any],
    ) -> Optional[str]:
        print(
            f"[AI_STUB] log_signal_from_engine: {symbol} {timeframe} {side} | "
            f"strategy={strategy_role} sub_uid={sub_uid} (ai_store not available)"
        )
        return None

log_signal_from_engine = _real_log_signal_from_engine

# ---------- Strategy registry ----------
try:
    from app.core import strategies as stratreg
    _HAS_STRATEGY_REGISTRY = True
except Exception:
    stratreg = None  # type: ignore
    _HAS_STRATEGY_REGISTRY = False

# ---------- Paths & env ----------
THIS_FILE = Path(__file__).resolve()
BOTS_DIR = THIS_FILE.parent
APP_DIR = BOTS_DIR.parent
ROOT_DIR = APP_DIR.parent

os.chdir(ROOT_DIR)

ENV_PATH = ROOT_DIR / ".env"
load_dotenv(dotenv_path=ENV_PATH)

BYBIT_BASE = os.getenv("BYBIT_BASE", "https://api.bybit.com").rstrip("/")


def _parse_bool(val: Optional[str], default: bool) -> bool:
    if val is None:
        return default
    v = val.strip().lower()
    if v in ("1", "true", "yes", "y", "on"):
        return True
    if v in ("0", "false", "no", "n", "off"):
        return False
    return default


SIG_ENABLED = _parse_bool(os.getenv("SIG_ENABLED"), True)
SIG_DRY_RUN = _parse_bool(os.getenv("SIG_DRY_RUN"), True)
SIG_USE_STRATEGIES = _parse_bool(os.getenv("SIG_USE_STRATEGIES"), True)
SIG_ALLOW_FALLBACK = _parse_bool(os.getenv("SIG_ALLOW_FALLBACK"), False)
SIG_FALLBACK_FANOUT = _parse_bool(os.getenv("SIG_FALLBACK_FANOUT"), False)

_raw_symbols = os.getenv("SIG_SYMBOLS", "BTCUSDT,ETHUSDT")
SIG_SYMBOLS: List[str] = [s.strip().upper() for s in _raw_symbols.split(",") if s.strip()]

_raw_tfs = os.getenv("SIG_TIMEFRAMES", "5,15")
SIG_TIMEFRAMES: List[str] = [tf.strip() for tf in _raw_tfs.split(",") if tf.strip()]

SIG_POLL_SEC = int(os.getenv("SIG_POLL_SEC", "15"))
SIG_HEARTBEAT_SEC = int(os.getenv("SIG_HEARTBEAT_SEC", "300"))

MA_LOOKBACK = 8

SIGNALS_DIR = ROOT_DIR / "signals"
SIGNALS_DIR.mkdir(parents=True, exist_ok=True)
SIGNALS_PATH = SIGNALS_DIR / "observed.jsonl"

tg = get_notifier("main")


def tg_info(msg: str) -> None:
    try:
        tg.info(msg)
    except Exception:
        print(f"[signal_engine][TG info fallback] {msg}")


def tg_error(msg: str) -> None:
    try:
        tg.error(msg)
    except Exception:
        print(f"[signal_engine][TG error fallback] {msg}")


def fetch_recent_klines(symbol: str, interval: str, limit: int = 20) -> List[Dict[str, Any]]:
    url = f"{BYBIT_BASE}/v5/market/kline"
    params = {
        "category": "linear",
        "symbol": symbol,
        "interval": interval,
        "limit": str(limit),
    }

    if _HAS_REQUESTS:
        resp = requests.get(url, params=params, headers={'User-Agent':'Mozilla/5.0'}, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    else:
        query = _urllib_parse.urlencode(params)  # type: ignore[name-defined]
        full_url = f"{url}?{query}"
        with _urllib_request.urlopen(full_url, timeout=10) as fh:  # type: ignore[name-defined]
            if hasattr(fh, "getcode"):
                status = fh.getcode()
                if status >= 400:
                    raise RuntimeError(f"HTTP error {status} when fetching {full_url}")
            body = fh.read()
            data = _json_fallback.loads(body.decode("utf-8"))

    if data.get("retCode") != 0:
        raise RuntimeError(f"Bybit kline error {data.get('retCode')}: {data.get('retMsg')}")

    raw_list = data.get("result", {}).get("list", []) or []

    if not raw_list:
        # Empty list usually means symbol/category mismatch or delisted instrument
        raise RuntimeError(
            f"Empty kline list for symbol={symbol} interval={interval} category={params.get('category')} retMsg={data.get('retMsg')}"
        )

    klines: List[Dict[str, Any]] = []
    for row in raw_list:
        ts_ms = int(row[0])
        o = float(row[1])
        h = float(row[2])
        low = float(row[3])
        c = float(row[4])
        v = float(row[5])
        klines.append({"ts_ms": ts_ms, "open": o, "high": h, "low": low, "close": c, "volume": v})

    klines.sort(key=lambda x: x["ts_ms"])
    return klines


def compute_simple_signal(candles: List[Dict[str, Any]]) -> Tuple[Optional[str], Dict[str, Any]]:
    debug: Dict[str, Any] = {}

    if len(candles) < 3:
        debug["reason"] = "not_enough_candles"
        return None, debug

    last = candles[-1]
    prev = candles[-2]

    closes = [c["close"] for c in candles[-MA_LOOKBACK:]]
    ma = sum(closes) / len(closes)

    debug.update({"last_close": last["close"], "prev_close": prev["close"], "ma": ma})

    side: Optional[str] = None
    if last["close"] > prev["close"] and last["close"] > ma:
        side = "LONG"
        debug["reason"] = "close_up_above_ma"
    elif last["close"] < prev["close"] and last["close"] < ma:
        side = "SHORT"
        debug["reason"] = "close_down_below_ma"
    else:
        debug["reason"] = "no_clear_edge"

    return side, debug


# FORCE_DEBUG_CALLSITE_V1: ensure observed.jsonl debug always has real numbers when candles are available
def _ensure_debug(candles, dbg):
    """
    Ensure we always have last_close/prev_close/ma,
    but DO NOT drop existing keys like 'regime', 'setup', 'signal_origin'.
    """
    base = dbg if isinstance(dbg, dict) else {}
    try:
        lc = base.get('last_close')
        pc = base.get('prev_close')
        ma = base.get('ma')
        if (lc is not None) and (pc is not None) and (ma is not None):
            return base

        _side, computed = compute_simple_signal(candles)
        if isinstance(computed, dict) and computed:
            merged = dict(base)
            for k in ('last_close','prev_close','ma'):
                if merged.get(k) is None and computed.get(k) is not None:
                    merged[k] = computed.get(k)
            if not merged.get('reason') and computed.get('reason'):
                merged['reason'] = computed.get('reason')
            return merged
    except Exception:
        pass
    return base

def _setup_from_simple(setup_name: str):
    def _fn(candles):
        side, dbg = compute_simple_signal(candles)
        if not side:
            return None, ""
        return side, str(dbg.get("reason", setup_name))
    return _fn

SETUP_LOGIC = {
    "breakout_high": _setup_from_simple("breakout_high"),
    "breakout_range": _setup_from_simple("breakout_range"),
    "dump_fade_reversion": _setup_from_simple("dump_fade_reversion"),
    "ema_trend_follow": _setup_from_simple("ema_trend_follow"),
    "failed_breakout_fade": _setup_from_simple("failed_breakout_fade"),
    "intraday_range_fade": _setup_from_simple("intraday_range_fade"),
    "mm_reversion_micro": _setup_from_simple("mm_reversion_micro"),
    "mm_spread_capture": _setup_from_simple("mm_spread_capture"),
    "pump_chase_momo": _setup_from_simple("pump_chase_momo"),
    "scalp_liquidity_sweep": _setup_from_simple("scalp_liquidity_sweep"),
    "scalp_reversal_snapback": _setup_from_simple("scalp_reversal_snapback"),
    "scalp_trend_continuation": _setup_from_simple("scalp_trend_continuation"),
    "squeeze_release": _setup_from_simple("squeeze_release"),
    "swing_reversion_channel": _setup_from_simple("swing_reversion_channel"),
    "swing_reversion_extreme": _setup_from_simple("swing_reversion_extreme"),
    "swing_trend_continuation": _setup_from_simple("swing_trend_continuation"),
    "swing_trend_follow": _setup_from_simple("swing_trend_follow"),
    "trend_breakout_retest": _setup_from_simple("trend_breakout_retest"),
    "trend_pullback": _setup_from_simple("trend_pullback"),
}


def tf_display(tf: str) -> str:
    if tf.endswith(("m", "h", "d")):
        return tf
    return f"{tf}m"


def _slug_from_reason(prefix: str, side_text: str, reason: str) -> str:
    base = reason.strip().lower().replace(" ", "_")
    return f"{prefix}_{side_text.lower()}_{base}"


def append_signal_jsonl(
    *,
    symbol: str,
    side_text: str,
    tf_label: str,
    bar_ts: int,
    price: Optional[float],
    reason: str,
    debug: Dict[str, Any],
    sub_uid: Optional[str] = None,
    strategy_name: Optional[str] = None,
) -> None:
    if side_text not in ("LONG", "SHORT"):
        return

    side_exec = "Buy" if side_text == "LONG" else "Sell"
    setup_type = _slug_from_reason("ma", side_text, reason)
    payload: Dict[str, Any] = {
        "symbol": symbol,
        "side": side_exec,
        "timeframe": tf_label,
        "reason": reason,
        "setup_type": setup_type,
        "ts_ms": bar_ts,
        "est_rr": 0.25,
        "debug": {
            "engine": "signal_engine_v2",
            "raw_reason": reason,
            "regime": debug.get("regime"),
            "last_close": debug.get("last_close"),
            "prev_close": debug.get("prev_close"),
            "ma": debug.get("ma"),
        },
    }

    if price is not None:
        payload["price"] = float(price)

    if sub_uid is not None:
        payload["sub_uid"] = str(sub_uid)
    if strategy_name is not None:
        payload["strategy_name"] = strategy_name

    try:
        with SIGNALS_PATH.open("a", encoding="utf-8") as f:
            f.write(_jsonlib.dumps(payload, separators=(",", ":"), ensure_ascii=False))
            f.write("\n")
    except Exception as e:
        print(f"[signal_engine] Failed to append to {SIGNALS_PATH}: {e}")


def build_universe() -> Dict[Tuple[str, str], List[Dict[str, Any]]]:
    universe: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}

    if SIG_USE_STRATEGIES and _HAS_STRATEGY_REGISTRY:
        try:
            sub_strats = stratreg.all_sub_strategies()
        except Exception as e:
            print(f"[signal_engine] Failed to load strategy config: {e}")
            sub_strats = []

        for s in sub_strats:
            sub_uid_raw = _get_strat_attr(s, "sub_uid", "")
            sub_uid = str(sub_uid_raw).strip()
            if not sub_uid:
                continue

            name_raw = _get_strat_attr(s, "name", None)
            name = str(name_raw) if name_raw is not None else f"sub-{sub_uid}"

            symbols = _get_strat_attr(s, "symbols", []) or []
            tfs = _get_strat_attr(s, "timeframes", []) or []

            if not symbols or not tfs:
                continue

            automation_mode = _get_strat_attr(s, "automation_mode", None)

            for sym in symbols:
                sym_u = str(sym).upper()
                for tf in tfs:
                    tf_str = str(tf).strip()
                    key = (sym_u, tf_str)
                    entry = {
                        "sub_uid": sub_uid,
                        "name": name,
                        "symbols": symbols,
                        "timeframes": tfs,
                        "automation_mode": automation_mode,
                        "raw": s,  # may be object OR dict, we handle both
                    }
                    universe.setdefault(key, []).append(entry)

    if not universe:
        for sym in SIG_SYMBOLS:
            for tf in SIG_TIMEFRAMES:
                universe.setdefault((sym, tf), [])

    return universe


def main() -> None:
    universe = build_universe()

    # PRUNE_DEAD_SYMBOLS (startup sanity): drop symbols that return *consistently empty* klines.
    # IMPORTANT:
    # - Only prune on our explicit "Empty kline list ..." error
    # - Do NOT prune on transient HTTP/network/timeouts/rate-limits
    dead_syms = set()
    try:
        probe_tf = "5"
        # GATE_PRUNE_DEAD_SYMBOLS_V1
        probe_syms = ([] if os.getenv('SIG_PRUNE_DEAD_SYMBOLS','0') != '1' else sorted({k[0] for k in universe.keys()}))
        for sym in probe_syms:
            empty_hits = 0
            for _attempt in range(2):
                try:
                    _ = fetch_recent_klines(sym, probe_tf, limit=10)
                    break
                except Exception as e:
                    msg = str(e)
                    if "Empty kline list" in msg:
                        empty_hits += 1
                        continue
                    # Non-empty exceptions are treated as transient: keep symbol
                    print(f"[WARN] Dead-symbol probe transient error (keeping {sym}): {type(e).__name__}: {e}")
                    break
            if empty_hits >= 2:
                dead_syms.add(sym)

        if dead_syms:
            print(f"[WARN] Pruning dead symbols (empty klines x2): {sorted(dead_syms)}")
            universe = {k: v for k, v in universe.items() if k[0] not in dead_syms}
    except Exception as _e:
        print(f"[WARN] Dead-symbol prune failed (non-fatal): {type(_e).__name__}: {_e}")




    all_symbols = sorted({k[0] for k in universe.keys()})
    all_tfs = sorted({k[1] for k in universe.keys()})

    print("=== Flashback Signal Engine v2 (Strategy-aware, AI-logging + JSONL) ===")
    print(f"Project root: {ROOT_DIR}")
    print(f"Using .env:   {ENV_PATH} (exists={ENV_PATH.exists()})")
    print(f"Bybit base:   {BYBIT_BASE}")
    print(f"SIG_ENABLED:  {SIG_ENABLED}")
    print(f"SIG_DRY_RUN:  {SIG_DRY_RUN}")
    print(f"SIG_USE_STRATEGIES: {SIG_USE_STRATEGIES} (has_registry={_HAS_STRATEGY_REGISTRY})")
    print(f"AI logging available: {_HAS_AI_HOOKS}")
    print(f"Universe symbols:    {all_symbols}")
    print(f"Universe timeframes: {all_tfs}")
    print(f"Poll every:          {SIG_POLL_SEC} sec")
    print(f"Heartbeat:           {SIG_HEARTBEAT_SEC} sec")
    print(f"JSONL path:          {SIGNALS_PATH}")

    if not SIG_ENABLED:
        msg = "⚠️ Signal engine is disabled (SIG_ENABLED=false). Exiting."
        print(msg)
        tg_info(msg)
        return

    if not universe:
        msg = "⚠️ Signal engine universe is empty (no symbols/timeframes). Exiting."
        print(msg)
        tg_info(msg)
        return

    if SIG_USE_STRATEGIES and _HAS_STRATEGY_REGISTRY:
        try:
            strat_objs = stratreg.all_sub_strategies()
            strat_names: List[str] = []
            for s in strat_objs:
                n = _get_strat_attr(s, "name", None)
                if not n:
                    su = _get_strat_attr(s, "sub_uid", None)
                    n = f"sub-{su}"
                strat_names.append(str(n))
        except Exception:
            strat_names = []
        tg_info(
            "🚀 Signal Engine v2 started (strategy-aware).\n"
            f"Symbols: {', '.join(all_symbols)}\n"
            f"TFs: {', '.join(tf_display(t) for t in all_tfs)}\n"
            f"Strategies: {', '.join(strat_names) or 'n/a'}\n"
            f"Poll: {SIG_POLL_SEC}s | Heartbeat: {SIG_HEARTBEAT_SEC}s\n"
            f"AI logging: {'on' if _HAS_AI_HOOKS else 'OFF (stub)'}"
        )
    else:
        tg_info(
            "🚀 Signal Engine v2 started (fallback mode, .env universe).\n"
            f"Symbols: {', '.join(all_symbols)}\n"
            f"TFs: {', '.join(tf_display(t) for t in all_tfs)}\n"
            f"Poll: {SIG_POLL_SEC}s | Heartbeat: {SIG_HEARTBEAT_SEC}s\n"
            f"AI logging: {'on' if _HAS_AI_HOOKS else 'OFF (stub)'}"
        )

    last_signal_bar: Dict[Tuple[str, str], int] = {}

    start_ts = time.time()
    next_heartbeat = start_ts + SIG_HEARTBEAT_SEC

    while True:
        # DBG_LOOP_START_V1
        print("[DBG] LOOP_START (entered while loop)", flush=True)
        loop_start = time.time()
        total_signals_this_loop = 0
        seen_emit = set()  # DEDUPE_EMIT_V1: prevent duplicate observed.jsonl rows per run


        for (symbol, tf), strat_list in universe.items():
            key = (symbol, tf)

            try:
                candles = fetch_recent_klines(symbol, tf, limit=max(MA_LOOKBACK + 2, 10))
            except Exception as e:
                print(f"[WARN] Failed to fetch klines for {symbol} {tf}: {type(e).__name__}: {e}")
                tg_error(f"⚠️ Signal engine kline error for {symbol} {tf}: {type(e).__name__}")
                continue

            if len(candles) < 3:
                print(f"[INFO] Not enough candles yet for {symbol} {tf}")
                continue

            latest_bar = candles[-1]
            bar_ts = latest_bar["ts_ms"]
            last_close = latest_bar["close"]

            last_ts = last_signal_bar.get(key)
            if last_ts is not None and bar_ts <= last_ts:
                continue

            regime_ind = compute_regime_indicators(candles)
            tf_label = tf_display(tf)

            # Collect per-strategy matches (NO global fanout)
            matched: List[Dict[str, Any]] = []
            # DBG_UNIVERSE_ONCE_V1
            if os.getenv('SIG_DBG_UNIVERSE','0') == '1':
                if not hasattr(main, '_dbg_universe_once'):
                    main._dbg_universe_once = True
                    try:
                        print(f'[DBG] universe key={symbol} tf={tf_label} strat_list_len={len(strat_list) if strat_list else 0}', flush=True)
                        if strat_list:
                            for j, strat in enumerate(strat_list[:3], start=1):
                                raw_obj = strat.get('raw')
                                print(f'[DBG] strat#{j} keys={sorted(list(strat.keys()))}', flush=True)
                                print(f'[DBG] strat#{j} raw_type={type(raw_obj).__name__} raw_keys={(sorted(list(raw_obj.keys())) if isinstance(raw_obj, dict) else None)}', flush=True)
                                st = _get_strat_attr(raw_obj, 'setup_types', []) or []
                                print(f'[DBG] strat#{j} setup_types_type={type(st).__name__} setup_types={st}', flush=True)
                    except Exception as e:
                        print(f'[DBG] universe debug failed: {type(e).__name__}: {e}', flush=True)
            # STRAT_COUNTERS_V1
            c_setup_types_empty = 0
            c_setups_checked = 0
            c_missing_logic = 0
            c_logic_none = 0
            c_regime_blocked = 0
            c_matched_added = 0

            # 1) Strategy setups with regime gating (object-safe) - per sub/strategy
            if strat_list:
                for strat in strat_list:
                    raw_obj = strat.get("raw")
                    setup_types = _get_strat_attr(raw_obj, "setup_types", []) or []
                    if not setup_types:
                        c_setup_types_empty += 1
                    if not isinstance(setup_types, list):
                        setup_types = []

                    for setup in setup_types:
                        c_setups_checked += 1
                        logic_fn = SETUP_LOGIC.get(str(setup))
                        if not logic_fn:
                            c_missing_logic += 1
                            continue
                        s_side, s_reason = logic_fn(candles)
                        if not s_side:
                            c_logic_none += 1
                            continue

                        if not passes_regime_filters(raw_obj, regime_ind):
                            c_regime_blocked += 1  # COUNTERS_CLEANUP_V1
                            continue

                        m_debug: Dict[str, Any] = {"setup": setup, "regime": regime_ind, "signal_origin": "strategy"}
                        c_matched_added += 1
                        matched.append({"strat": strat, "side": s_side, "reason": f"{setup}:{s_reason}", "debug": m_debug})
                        break  # one setup per strategy per bar

            # 2) Fallback (only if allowed) and ONLY if nothing matched
            fallback_payload = None
            if (not matched) and SIG_ALLOW_FALLBACK:
                simple_side, simple_debug = compute_simple_signal(candles)
                if simple_side:
                    fb_debug = dict(simple_debug)
                    fb_debug["signal_origin"] = "fallback"
                    fb_debug["regime"] = regime_ind
                    fallback_payload = {"side": simple_side, "reason": f"fallback:{simple_debug.get('reason')}", "debug": fb_debug}

            # If nothing at all, mark bar processed and continue
            if (not matched) and (fallback_payload is None):
                last_signal_bar[key] = bar_ts
                continue

            # Mark bar processed for this (symbol, tf) regardless
            last_signal_bar[key] = bar_ts

            # Emit matched strategies (strategy-only)
            if matched:
                for item in matched:
                    strat = item["strat"]
                    side = item["side"]
                    reason = item["reason"]
                    debug = item["debug"]

                    sub_uid = str(strat.get("sub_uid"))
                    _dk = (sub_uid, symbol, tf_label, bar_ts, side, reason)
                    if _dk in seen_emit:
                        continue
                    seen_emit.add(_dk)  # DEDUPE_EMIT_V1

                    strat_name = strat.get("name", f"sub-{sub_uid}")
                    automation_mode = strat.get("automation_mode")

                    msg = (
                        f"📡 *Signal Engine v2* - {symbol} / {tf_label}\n"
                        f"Side: *{side}*\n"
                        f"Strategy: `{strat_name}`\n"
                        f"Sub UID: `{sub_uid}`\n"
                        f"Last close: `{last_close}`\n"
                        f"Reason: `{reason}`\n"
                        f"(No orders placed here; executors handle trades.)"
                    )
                    tg_info(msg)

                    regime_tags = [reason]
                    base_extra = {"engine": "signal_engine_v2", "raw_debug": debug, "tf_raw": tf}

                    try:
                        sub_label = stratreg.get_sub_label(sub_uid) if _HAS_STRATEGY_REGISTRY else None
                    except Exception:
                        sub_label = None

                    extra = dict(base_extra)
                    extra.update(
                        {
                            "strategy_name": strat_name,
                            "strategy_automation_mode": automation_mode,
                            "sub_uid": sub_uid,
                            "sub_label": sub_label,
                            "strategy_raw": strat.get("raw") or strat,
                        }
                    )

                    signal_id = log_signal_from_engine(
                        symbol=symbol,
                        timeframe=tf_label,
                        side=side,
                        source="signal_engine_v2",
                        confidence=None,
                        stop_hint=None,
                        owner="AUTO_STRATEGY",
                        sub_uid=sub_uid,
                        strategy_role=strat_name,
                        regime_tags=regime_tags,
                        extra=extra,
                    )
                    print(
                        f"[SIGNAL] {symbol} {tf_label} {side} | "
                        f"strategy={strat_name} sub_uid={sub_uid} | signal_id={signal_id} | reason={reason}"
                    )

                    append_signal_jsonl(
                        symbol=symbol,
                        side_text=side,
                        tf_label=tf_label,
                        bar_ts=bar_ts,
                        price=last_close,
                        reason=reason,
                        debug=_ensure_debug(candles, debug),  # FORCE_DEBUG_CALLSITE_V1
                        sub_uid=sub_uid,
                        strategy_name=strat_name,
                    )

                    total_signals_this_loop += 1

            # Emit fallback (generic or fanout, depending on env)
            elif fallback_payload is not None:
                side = fallback_payload["side"]
                reason = fallback_payload["reason"]
                debug = fallback_payload["debug"]

                msg = (
                    f"📡 *Signal Engine v2* - {symbol} / {tf_label}\n"
                    f"Side: *{side}*\n"
                    f"Last close: `{last_close}`\n"
                    f"Reason: `{reason}`\n"
                    f"(Fallback mode)\n"
                    f"(No orders placed here; executors handle trades.)"
                )
                tg_info(msg)

                regime_tags = [reason]
                base_extra = {"engine": "signal_engine_v2", "raw_debug": debug, "tf_raw": tf}

                if SIG_FALLBACK_FANOUT and strat_list:
                    for strat in (strat_list or []):
                        sub_uid = str(strat.get("sub_uid"))
                        strat_name = strat.get("name", f"sub-{sub_uid}")
                        automation_mode = strat.get("automation_mode")

                        try:
                            sub_label = stratreg.get_sub_label(sub_uid) if _HAS_STRATEGY_REGISTRY else None
                        except Exception:
                            sub_label = None

                        extra = dict(base_extra)
                        extra.update(
                            {
                                "strategy_name": strat_name,
                                "strategy_automation_mode": automation_mode,
                                "sub_uid": sub_uid,
                                "sub_label": sub_label,
                                "strategy_raw": strat.get("raw") or strat,
                            }
                        )

                        signal_id = log_signal_from_engine(
                            symbol=symbol,
                            timeframe=tf_label,
                            side=side,
                            source="signal_engine_v2",
                            confidence=None,
                            stop_hint=None,
                            owner="AUTO_STRATEGY",
                            sub_uid=sub_uid,
                            strategy_role=strat_name,
                        regime_tags=regime_tags,
                            extra=extra,
                        )
                        print(
                            f"[SIGNAL] {symbol} {tf_label} {side} | "
                            f"strategy={strat_name} sub_uid={sub_uid} | signal_id={signal_id} | reason={reason}"
                        )

                        append_signal_jsonl(
                            symbol=symbol,
                            side_text=side,
                            tf_label=tf_label,
                            bar_ts=bar_ts,
                            price=last_close,
                            reason=reason,
                            debug=_ensure_debug(candles, debug),
                            sub_uid=sub_uid,
                            strategy_name=strat_name,
                        )

                        total_signals_this_loop += 1

                else:
                    extra = dict(base_extra)
                    extra.update({"strategy_name": None, "strategy_automation_mode": None})

                    signal_id = log_signal_from_engine(
                        symbol=symbol,
                        timeframe=tf_label,
                        side=side,
                        source="signal_engine_v2",
                        confidence=None,
                        stop_hint=None,
                        owner="AUTO_STRATEGY",
                        sub_uid=None,
                        strategy_role="GENERIC_SIGNAL_ENGINE",
                        regime_tags=regime_tags,
                        extra=extra,
                    )
                    print(f"[SIGNAL] {symbol} {tf_label} {side} | strategy=GENERIC | signal_id={signal_id} | reason={reason}")

                    append_signal_jsonl(
                        symbol=symbol,
                        side_text=side,
                        tf_label=tf_label,
                        bar_ts=bar_ts,
                        price=last_close,
                        reason=reason,
                        debug=_ensure_debug(candles, debug),
                        sub_uid=None,
                        strategy_name=None,
                    )

                    total_signals_this_loop += 1
        now = time.time()
        if now >= next_heartbeat:
            uptime_min = int((now - start_ts) / 60)
            hb = (
                f"🩺 Signal Engine heartbeat (v2)\n"
                f"- Uptime: {uptime_min} min\n"
                f"- Symbols: {', '.join(all_symbols)}\n"
                f"- TFs: {', '.join(tf_display(t) for t in all_tfs)}\n"
                f"- Last loop signals: {total_signals_this_loop}\n"
                f"- Using strategies: {SIG_USE_STRATEGIES and _HAS_STRATEGY_REGISTRY}\n"
                f"- AI logging: {'on' if _HAS_AI_HOOKS else 'OFF (stub)'}"
            )
            tg_info(hb)
            next_heartbeat = now + SIG_HEARTBEAT_SEC

        sleep_for = max(1.0, SIG_POLL_SEC - (time.time() - loop_start))
        # WARN_TUNE_V1: no-match is normal. Warn only on suspicious conditions.
        if c_setups_checked > 0 and c_matched_added == 0:
            suspicious = (
                (c_missing_logic > 0) or
                (c_setup_types_empty > 0) or
                (c_regime_blocked == c_setups_checked and c_setups_checked >= 3)
            )
            if suspicious:
                print(
                    f"[WARN] suspicious_no_match setups_checked={c_setups_checked} missing_logic={c_missing_logic} "
                    f"logic_none={c_logic_none} regime_blocked={c_regime_blocked} setup_types_empty={c_setup_types_empty}",
                    flush=True,
                )
        print(f"[DBG] setups_checked={c_setups_checked} missing_logic={c_missing_logic} logic_none={c_logic_none} setup_types_empty={c_setup_types_empty} matched_added={c_matched_added}", flush=True)
        time.sleep(sleep_for)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[EXIT] Signal Engine interrupted by user.")
