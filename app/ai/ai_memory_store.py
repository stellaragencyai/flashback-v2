#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Memory Store (Rollups Read Path) v2.1 ✅ Phase 6-ready

Fix v2.1
- policy_hash matching is now prefix-tolerant when the incoming hash is short.
  This prevents strict equality misses when upstream only provides a short prefix
  (e.g., 12 chars) but rollups store the full hash.

Behavior
- Reads aggregated rollups from: state/ai_memory/memory_rollups.sqlite
- Provides tiered query:
    Tier A: symbol-scoped (symbol=<SYMBOL>)
    Tier B: ANY fallback
- Strict policy + timeframe by default
- Matches by memory_id == setup_event.payload.features.memory_fingerprint
  (supports prefix match if fingerprint is shorter)

Why
- Rollups are the production substrate: stable schema + cheap queries
"""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from app.core.config import settings  # type: ignore
    ROOT: Path = settings.ROOT  # type: ignore
except Exception:  # pragma: no cover
    ROOT = Path(__file__).resolve().parents[2]

STATE_DIR: Path = ROOT / "state"
AI_MEMORY_DIR: Path = STATE_DIR / "ai_memory"
ROLLUPS_DB: Path = AI_MEMORY_DIR / "memory_rollups.sqlite"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _normalize_timeframe(tf: Any) -> Optional[str]:
    if tf is None:
        return None
    try:
        s = str(tf).strip().lower()
    except Exception:
        return None
    if not s:
        return None
    if s.endswith(("m", "h", "d", "w")):
        return s
    try:
        n = int(float(s))
        if n > 0:
            return f"{n}m"
    except Exception:
        return None
    return None


def _normalize_symbol(sym: Any) -> Optional[str]:
    if sym is None:
        return None
    try:
        s = str(sym).strip().upper()
    except Exception:
        return None
    return s or None


def _safe_str(x: Any) -> str:
    try:
        return str(x).strip()
    except Exception:
        return ""


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _looks_like_full_hash(s: str) -> bool:
    """
    We store full policy hashes upstream (typically long hex).
    If caller passes only a prefix (often 12), strict equality will miss.
    Treat anything "short" as a prefix.
    """
    s = (s or "").strip()
    # 64-hex is common, but we accept "long enough" as full.
    return len(s) >= 24


@dataclass(frozen=True)
class QueryOptions:
    k: int = 25
    min_n: int = 3
    min_n_symbol: Optional[int] = None
    min_n_any: Optional[int] = None

    policy_match: str = "strict"       # "strict" | "off"
    timeframe_match: str = "strict"    # "strict" | "off"
    max_age_days: int = 180

    prefer_symbol_scope: bool = True
    allow_any_fallback: bool = True


def _effective_min_n(opts: QueryOptions, tier: str) -> int:
    if tier == "A" and opts.min_n_symbol is not None:
        return max(1, int(opts.min_n_symbol))
    if tier == "B" and opts.min_n_any is not None:
        return max(1, int(opts.min_n_any))
    if tier == "A":
        return 1
    return max(1, int(opts.min_n))


def _get_setup_fields(setup_event: Dict[str, Any]) -> Tuple[str, str, str, str, str]:
    """
    Return (policy_hash, timeframe, setup_type, symbol, memory_fingerprint)
    """
    policy_hash = ""
    timeframe = ""
    setup_type = ""
    symbol = ""
    mem_fp = ""

    if isinstance(setup_event.get("policy"), dict):
        policy_hash = _safe_str(setup_event["policy"].get("policy_hash"))

    timeframe = _safe_str(_normalize_timeframe(setup_event.get("timeframe")) or "")
    setup_type = _safe_str(setup_event.get("setup_type") or "")
    symbol = _safe_str(_normalize_symbol(setup_event.get("symbol")) or "")

    payload = setup_event.get("payload") if isinstance(setup_event.get("payload"), dict) else {}
    features = payload.get("features") if isinstance(payload.get("features"), dict) else {}
    mem_fp = _safe_str(features.get("memory_fingerprint") or "")

    # fallback timeframe from payload.extra.timeframe
    if not timeframe:
        extra = payload.get("extra") if isinstance(payload.get("extra"), dict) else {}
        timeframe = _safe_str(_normalize_timeframe(extra.get("timeframe")) or "")

    return (policy_hash, timeframe, setup_type, symbol, mem_fp)


def _row_to_memory(r: sqlite3.Row) -> Dict[str, Any]:
    """
    Return the memory dict shape expected by ai_gatekeeper:
      memory["stats"]["n,wins,losses,r_mean,r_sum"] etc.
    """
    n = int(r["n"] or 0)
    wins = int(r["wins"] or 0)
    losses = int(r["losses"] or 0)

    avg_r = float(r["avg_r_multiple"] or 0.0)
    r_sum = avg_r * float(n)

    out: Dict[str, Any] = {
        "memory_id": r["memory_id"],
        "symbol": r["symbol"],
        "timeframe": r["timeframe"],
        "setup_type": r["setup_type"],
        "policy_hash": r["policy_hash"],
        "last_ts_ms": int(r["last_ts_ms"] or 0),
        "stats": {
            "n": n,
            "wins": wins,
            "losses": losses,
            "r_mean": float(avg_r),
            "r_sum": float(r_sum),
            "win_rate": r["win_rate"],
            "avg_pnl_usd": r["avg_pnl_usd"],
            "allow_rate": r["allow_rate"],
            "avg_size_multiplier": r["avg_size_multiplier"],
        },
    }
    return out


def _query_rollups(
    *,
    memory_id: str,
    symbol: Optional[str],
    timeframe: Optional[str],
    setup_type: Optional[str],
    policy_hash: Optional[str],
    min_n: int,
    max_age_days: int,
    k: int,
) -> List[Dict[str, Any]]:
    if not ROLLUPS_DB.exists():
        return []

    max_age_ms = int(max(1, max_age_days) * 24 * 60 * 60 * 1000)
    now = _now_ms()

    clauses = ["memory_id LIKE :mem_like", "n >= :min_n"]
    params: Dict[str, Any] = {
        "mem_like": (memory_id + "%") if len(memory_id) < 64 else memory_id,
        "min_n": int(min_n),
        "age_cutoff": int(now - max_age_ms),
        "k": int(max(1, k)),
    }

    # age filter
    clauses.append("last_ts_ms >= :age_cutoff")

    if symbol:
        clauses.append("symbol = :symbol")
        params["symbol"] = symbol

    if timeframe:
        clauses.append("timeframe = :timeframe")
        params["timeframe"] = timeframe

    if setup_type:
        clauses.append("setup_type = :setup_type")
        params["setup_type"] = setup_type

    # ✅ FIX: policy hash can be short prefix from upstream
    if policy_hash:
        ph = str(policy_hash).strip()
        if _looks_like_full_hash(ph):
            clauses.append("policy_hash = :policy_hash")
            params["policy_hash"] = ph
        else:
            clauses.append("policy_hash LIKE :policy_hash_like")
            params["policy_hash_like"] = ph + "%"

    where_sql = " AND ".join(clauses)

    conn = _connect(ROLLUPS_DB)
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT
            memory_id, symbol, timeframe, setup_type, policy_hash,
            n, wins, losses,
            win_rate, avg_r_multiple, avg_pnl_usd,
            allow_rate, avg_size_multiplier,
            last_ts_ms, built_ts_ms
        FROM memory_rollups
        WHERE {where_sql}
        ORDER BY avg_r_multiple DESC, win_rate DESC, n DESC, last_ts_ms DESC
        LIMIT :k;
        """,
        params,
    )
    rows = cur.fetchall()
    conn.close()

    return [_row_to_memory(r) for r in rows]


def query_memories_tiered(setup_event: Dict[str, Any], opts: QueryOptions = QueryOptions()) -> Dict[str, Any]:
    """
    Tier A: symbol scoped
    Tier B: ANY fallback
    """
    policy_hash, tf, setup_type, sym, mem_fp = _get_setup_fields(setup_event)

    if not mem_fp:
        return {"ts": _now_ms(), "matched": [], "tier_used": "NONE", "meta": {"reason": "missing_memory_fingerprint"}}

    k = max(1, int(opts.k))
    tierA_min = _effective_min_n(opts, "A")
    tierB_min = _effective_min_n(opts, "B")

    strict_policy = (opts.policy_match == "strict")
    strict_tf = (opts.timeframe_match == "strict")

    ph = policy_hash if (strict_policy and policy_hash) else None
    tfn = tf if (strict_tf and tf) else None
    st = setup_type or None

    if opts.prefer_symbol_scope and sym:
        mA = _query_rollups(
            memory_id=mem_fp,
            symbol=sym,
            timeframe=tfn,
            setup_type=st,
            policy_hash=ph,
            min_n=tierA_min,
            max_age_days=int(opts.max_age_days),
            k=k,
        )
        if mA:
            return {
                "ts": _now_ms(),
                "matched": mA,
                "tier_used": "A",
                "meta": {
                    "min_n_effective": tierA_min,
                    "policy_match": opts.policy_match,
                    "timeframe_match": opts.timeframe_match,
                },
            }

    if opts.allow_any_fallback:
        mB = _query_rollups(
            memory_id=mem_fp,
            symbol=None,
            timeframe=tfn,
            setup_type=st,
            policy_hash=ph,
            min_n=tierB_min,
            max_age_days=int(opts.max_age_days),
            k=k,
        )
        if mB:
            return {
                "ts": _now_ms(),
                "matched": mB,
                "tier_used": "B",
                "meta": {
                    "min_n_effective": tierB_min,
                    "policy_match": opts.policy_match,
                    "timeframe_match": opts.timeframe_match,
                },
            }

    return {"ts": _now_ms(), "matched": [], "tier_used": "NONE", "meta": {"reason": "no_matches_after_tiers"}}


def query_memories(setup_event: Dict[str, Any], opts: QueryOptions = QueryOptions()) -> Dict[str, Any]:
    """
    Backward compatible: behaves like Tier B (ANY).
    """
    opts2 = QueryOptions(
        k=opts.k,
        min_n=opts.min_n,
        min_n_symbol=opts.min_n_symbol,
        min_n_any=opts.min_n_any,
        policy_match=opts.policy_match,
        timeframe_match=opts.timeframe_match,
        max_age_days=opts.max_age_days,
        prefer_symbol_scope=False,
        allow_any_fallback=True,
    )
    r = query_memories_tiered(setup_event, opts2)
    r["tier_used"] = "B" if r.get("matched") else "NONE"
    return r
