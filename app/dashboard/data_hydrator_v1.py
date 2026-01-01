# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ----------------------------
# Paths (relative to repo root when run from C:\flashback)
# ----------------------------
STATE_DIR = Path("state")
ORCH_STATE = STATE_DIR / "orchestrator_state.json"
OPS_SNAPSHOT = STATE_DIR / "ops_snapshot.json"
DASHBOARD_SNAPSHOT = STATE_DIR / "dashboard_snapshot.json"  # optional truth snapshot (outcomes-derived)

AI_EVENTS_DIR = STATE_DIR / "ai_events"
OUTCOMES_V1 = AI_EVENTS_DIR / "outcomes.v1.jsonl"
TRAINABLE_REBUILT = AI_EVENTS_DIR / "outcomes.v1.trainable.rebuilt.jsonl"
CUTOVER_PATH = AI_EVENTS_DIR / "training_cutover.json"

PAPER_DIR = STATE_DIR / "paper"

FLEET_MANIFEST = Path("config") / "fleet_manifest.yaml"

# schema v3: adds integrity_score, integrity_alerts, state_color, LTM, promotion ladder, decay, cull
SCHEMA_VERSION = 3


# ----------------------------
# helpers
# ----------------------------
def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_float(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0


def _safe_int(x: Any) -> int:
    try:
        return int(x)
    except Exception:
        return 0


def _safe_read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8", errors="ignore") or "{}")
    except Exception:
        return {}


def _safe_read_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    out: List[Dict[str, Any]] = []
    try:
        for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = (line or "").strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    out.append(obj)
            except Exception:
                continue
    except Exception:
        return []
    return out


def _safe_read_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        import yaml  # type: ignore

        raw = yaml.safe_load(path.read_text(encoding="utf-8", errors="ignore")) or {}
        return raw if isinstance(raw, dict) else {}
    except Exception:
        return {}


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _ms_to_iso(ts_ms: int | None) -> Optional[str]:
    if not ts_ms:
        return None
    try:
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts_ms / 1000.0))
    except Exception:
        return None


def _norm_account_label(v: Any) -> str:
    """
    Normalize account labels across sources.

    Examples:
      flashback_01 -> flashback01
      flashback01  -> flashback01
      FLASHBACK_1  -> flashback01
      main         -> main
    """
    s = str(v or "").strip()
    if not s:
        return ""

    s = s.lower().strip()

    if s == "main":
        return "main"

    if s.startswith("flashback_"):
        tail = s.replace("flashback_", "")
        try:
            n = int(tail)
            return f"flashback{n:02d}"
        except Exception:
            return s.replace("_", "")

    if s.startswith("flashback"):
        tail = s.replace("flashback", "").replace("_", "")
        if tail.isdigit():
            try:
                n = int(tail)
                return f"flashback{n:02d}"
            except Exception:
                return f"flashback{tail}"
        return s.replace("_", "")

    return s.replace("_", "")


def _is_valid_account_label(v: Any) -> bool:
    """
    Strict validator so we don't accidentally treat metadata keys as account ids.
    Allowed:
      - main
      - flashback01..flashback10 (and any flashbackNN where NN is digits)
    """
    s = _norm_account_label(v)
    if not s:
        return False
    if s == "main":
        return True
    if s.startswith("flashback"):
        tail = s.replace("flashback", "")
        return tail.isdigit() and len(tail) in (1, 2) or (tail.isdigit() and len(tail) >= 1)
    return False


def _expected_accounts() -> set[str]:
    out = {"main"}
    for n in range(1, 11):
        out.add(f"flashback{n:02d}")
    return out


def _extract_account_label_from_row(row: Dict[str, Any]) -> str:
    v = row.get("account_label", None)
    if v is None:
        v = row.get("account", None)
    return _norm_account_label(v)


def _extract_setup_type_from_row(row: Dict[str, Any]) -> str:
    st = row.get("setup_type", None)
    if st is None:
        st = row.get("setup", None)
    return str(st or "").strip() or "unknown"


def _extract_timeframe_from_row(row: Dict[str, Any]) -> str:
    tf = row.get("timeframe", None)
    if tf is None:
        tf = row.get("tf", None)
    return str(tf or "").strip() or "unknown"


def _extract_symbol_from_row(row: Dict[str, Any]) -> str:
    sym = row.get("symbol", None)
    return str(sym or "").strip() or "UNKNOWN"


def _extract_ts_ms_from_outcome(row: Dict[str, Any]) -> Optional[int]:
    for k in ("ts_ms", "closed_ts_ms", "closed_ms", "closed_ts"):
        if k in row and row.get(k) is not None:
            try:
                return int(row.get(k))
            except Exception:
                pass
    return None


def _get_cutover_ts_ms() -> Optional[int]:
    j = _safe_read_json(CUTOVER_PATH)
    ts = j.get("ts_ms", None)
    if ts is None:
        ts = j.get("cutover_ts_ms", None)
    try:
        return int(ts) if ts is not None else None
    except Exception:
        return None


def _load_manifest_index() -> Dict[str, Dict[str, Any]]:
    """
    Index fleet_manifest.yaml by normalized account_label.
    """
    m = _safe_read_yaml(FLEET_MANIFEST)
    fleet = m.get("fleet", [])
    idx: Dict[str, Dict[str, Any]] = {}
    if isinstance(fleet, list):
        for row in fleet:
            if not isinstance(row, dict):
                continue
            acct = _norm_account_label(row.get("account_label"))
            if acct:
                idx[acct] = row
    return idx


def _count_paper_closed(account_label: str) -> int:
    acct = _norm_account_label(account_label)
    candidates = [
        PAPER_DIR / f"{acct}.json",
        PAPER_DIR / f"{acct.replace('flashback', 'flashback_')}.json" if acct.startswith("flashback") else PAPER_DIR / f"{acct}.json",
    ]
    for p in candidates:
        j = _safe_read_json(p)
        closed = j.get("closed_trades", [])
        if isinstance(closed, list):
            return len(closed)
    return 0


def _pick_proc_heartbeat_ms(p: Dict[str, Any]) -> int:
    for k in ("last_heartbeat_ms", "last_seen_ms", "last_seen_ts_ms", "ts_ms", "started_ts_ms", "last_checked_ts_ms"):
        if k in p and p.get(k) is not None:
            try:
                return int(p.get(k))
            except Exception:
                pass
    return 0


def _normalize_ops_accounts(ops_raw: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    ops_snapshot.json can be shaped as:
      A) {"accounts": {...}}  (future)
      B) {"flashback_01": {...}, "version": 1, "updated_ms": ...} (current messy)
    Return only account-like keys, normalized.

    IMPORTANT: hard-filter junk keys before AND after normalization so we never
    accidentally emit fake "accounts" like components/version/updatedms.
    """
    if not isinstance(ops_raw, dict):
        return {}

    src = ops_raw.get("accounts", None)
    if not isinstance(src, dict):
        src = ops_raw

    out: Dict[str, Dict[str, Any]] = {}

    RAW_JUNK = {
        "components",
        "component",
        "version",
        "updated_ms",
        "updatedms",
        "updated",
        "schema_version",
        "schema",
        "meta",
        "global",
        "source",
        "accounts",  # sometimes people nest accidentally
    }

    for k, v in (src.items() if isinstance(src, dict) else []):
        k0 = str(k or "").strip().lower()
        if not k0:
            continue
        if k0 in RAW_JUNK:
            continue

        nk = _norm_account_label(k0)
        if not nk:
            continue

        # kill junk keys that normalize into nonsense
        if nk in ("version", "updatedms", "components", "accounts", "meta"):
            continue

        # strict allowlist pattern
        if not _is_valid_account_label(nk):
            continue

        out[nk] = v if isinstance(v, dict) else {}

    return out


def _classify_status(enabled: bool, online: bool, heartbeat_ms: int) -> str:
    if not enabled:
        return "DISABLED"
    if not online:
        return "OFFLINE"
    now = _now_ms()
    hb = _safe_int(heartbeat_ms)
    if hb <= 0:
        return "STALE"
    if (now - hb) > int(os.getenv("DASH_STALE_MS", "60000") or "60000"):
        return "STALE"
    return "ONLINE"


def _status_color(status: str, integrity_score: int, integrity_alerts: List[str]) -> str:
    """
    UI can map this to green/yellow/red.
    """
    s = (status or "").upper()
    if s in ("DISABLED",):
        return "gray"
    if s in ("OFFLINE",):
        return "red"
    if s in ("STALE",):
        return "yellow"
    # ONLINE -> use integrity
    if integrity_score >= int(os.getenv("DASH_GREEN_INTEGRITY_MIN", "90") or "90") and not integrity_alerts:
        return "green"
    if integrity_score >= int(os.getenv("DASH_YELLOW_INTEGRITY_MIN", "70") or "70"):
        return "yellow"
    return "red"


def _calc_integrity_score(join_pct: float, unknown_pct: float, continuity_nulls: int, orphans_count: int, outcomes_delta: int) -> int:
    score = 100.0

    # Joining matters most
    if join_pct < 100.0:
        score -= (100.0 - join_pct) * 1.5

    # Unknown setup types poison learning
    score -= unknown_pct * 2.0

    # Continuity nulls are unacceptable
    if continuity_nulls > 0:
        score -= 25.0 + (continuity_nulls * 2.0)

    # Orphans imply broken pairing coverage
    if orphans_count > 0:
        score -= min(20.0, orphans_count * 0.25)

    # outcomes_delta indicates mismatch
    if outcomes_delta != 0:
        score -= min(15.0, abs(outcomes_delta) * 1.0)

    return int(_clamp(score, 0.0, 100.0))


def _calc_integrity_alerts(join_pct: float, unknown_pct: float, continuity_nulls: int, orphans_count: int, outcomes_delta: int) -> List[str]:
    alerts: List[str] = []
    if join_pct < float(os.getenv("DASH_ALERT_MIN_JOIN_PCT", "95") or "95"):
        alerts.append("join_gap")
    if unknown_pct > float(os.getenv("DASH_ALERT_MAX_UNKNOWN_PCT", "1") or "1"):
        alerts.append("unknown_setup")
    if continuity_nulls > 0:
        alerts.append("continuity_nulls")
    if orphans_count > 0:
        alerts.append("orphans_present")
    if outcomes_delta != 0:
        alerts.append("outcomes_delta_nonzero")
    return alerts


def _bucket_key(symbol: str, timeframe: str, setup_type: str) -> str:
    return f"{symbol}|{timeframe}|{setup_type}"


def _bucket_tier(n: int, winrate: float, last_trade_age_sec: Optional[float], last_profit_age_sec: Optional[float]) -> str:
    """
    Deterministic tiering based on what we actually have now (outcomes).
    You can replace this later with a real promotion state machine.
    """
    # configurable thresholds
    n_obs = int(os.getenv("BUCKET_N_OBSERVE", "10") or "10")
    n_prob = int(os.getenv("BUCKET_N_PROBATION", "25") or "25")
    n_act = int(os.getenv("BUCKET_N_ACTIVE", "50") or "50")
    n_prime = int(os.getenv("BUCKET_N_PRIME", "120") or "120")

    wr_prob = float(os.getenv("BUCKET_WR_PROBATION", "0.52") or "0.52")
    wr_act = float(os.getenv("BUCKET_WR_ACTIVE", "0.55") or "0.55")
    wr_prime = float(os.getenv("BUCKET_WR_PRIME", "0.58") or "0.58")

    stale_trade = float(os.getenv("BUCKET_STALE_TRADE_SEC", str(7 * 24 * 3600)) or str(7 * 24 * 3600))
    stale_profit = float(os.getenv("BUCKET_STALE_PROFIT_SEC", str(14 * 24 * 3600)) or str(14 * 24 * 3600))

    # stale demotion
    if last_trade_age_sec is not None and last_trade_age_sec > stale_trade:
        return "DEPRECATED"
    if last_profit_age_sec is not None and last_profit_age_sec > stale_profit:
        return "DEPRECATED"

    if n < n_obs:
        return "COLD"
    if n < n_prob:
        return "OBSERVE"
    if n < n_act:
        return "PROBATION" if winrate >= wr_prob else "OBSERVE"
    if n < n_prime:
        return "ACTIVE" if winrate >= wr_act else "PROBATION"
    return "PRIME" if winrate >= wr_prime else "ACTIVE"


def _role_enforcement(role: str, tier_counts: Dict[str, int]) -> Tuple[bool, str]:
    """
    Minimal role enforcement now: ensure role is known and has expected bucket maturity distribution.
    """
    r = (role or "").strip().lower()
    if not r:
        return False, "role_missing"

    # Normalize common roles
    if r in ("explorer", "explore", "learning"):
        expected = "learning"
    elif r in ("refiner", "stabilize", "stabilizer"):
        expected = "refiner"
    elif r in ("exploiter", "extractor", "profit", "execution"):
        expected = "exploiter"
    else:
        return False, f"role_unknown:{role}"

    cold = tier_counts.get("COLD", 0)
    observe = tier_counts.get("OBSERVE", 0)
    probation = tier_counts.get("PROBATION", 0)
    active = tier_counts.get("ACTIVE", 0)
    prime = tier_counts.get("PRIME", 0)

    # simple sanity checks, not commandments
    if expected == "learning":
        if (cold + observe) < (active + prime):
            return False, "explorer_should_be_learning_heavy"
        return True, "ok"
    if expected == "refiner":
        if active < max(5, prime):
            return False, "refiner_should_have_more_active"
        return True, "ok"
    if expected == "exploiter":
        if prime < max(3, active // 3):
            return False, "exploiter_should_be_prime_heavy"
        return True, "ok"

    return True, "ok"


def _cull_recommendation(total_trades: int, winrate: float, integrity_score: int, integrity_alerts: List[str]) -> Tuple[bool, str]:
    """
    Automatic strategy cull recommendation (dashboard-only).
    No execution here, just truth + suggestion.
    """
    min_trades = int(os.getenv("CULL_MIN_TRADES", "30") or "30")
    min_wr = float(os.getenv("CULL_MIN_WINRATE", "0.40") or "0.40")  # 0.40 => 40%
    min_integrity = int(os.getenv("CULL_MIN_INTEGRITY", "60") or "60")

    if integrity_score < min_integrity:
        return True, "integrity_too_low"
    if integrity_alerts:
        return True, f"integrity_alerts:{','.join(integrity_alerts)}"
    if total_trades >= min_trades and winrate < min_wr:
        return True, f"winrate_too_low({winrate:.3f})"
    return False, "ok"


def _build_integrity_maps() -> Tuple[
    Dict[str, int],  # outcomes_count
    Dict[str, int],  # joined_count (proxy via trainable rebuilt)
    Dict[str, int],  # unknown_count
    Dict[str, int],  # continuity_nulls
]:
    cutover_ts = _get_cutover_ts_ms()
    outcomes = _safe_read_jsonl(OUTCOMES_V1)
    trainable = _safe_read_jsonl(TRAINABLE_REBUILT)

    outcomes_count: Dict[str, int] = {}
    unknown_count: Dict[str, int] = {}
    continuity_nulls: Dict[str, int] = {}

    for o in outcomes:
        if str(o.get("schema_version", "")).strip() != "outcome.v1":
            continue

        acct = _extract_account_label_from_row(o)
        if not acct:
            continue

        if cutover_ts is not None:
            ts = _extract_ts_ms_from_outcome(o)
            if ts is not None and ts < cutover_ts:
                continue

        outcomes_count[acct] = outcomes_count.get(acct, 0) + 1

        st = _extract_setup_type_from_row(o)
        if (not st) or (st.lower() == "unknown"):
            unknown_count[acct] = unknown_count.get(acct, 0) + 1

        ct = o.get("client_trade_id", None)
        stid = o.get("source_trade_id", None)
        if not ct or not stid:
            continuity_nulls[acct] = continuity_nulls.get(acct, 0) + 1

    joined_count: Dict[str, int] = {}
    for t in trainable:
        acct = _extract_account_label_from_row(t)
        if not acct:
            continue
        joined_count[acct] = joined_count.get(acct, 0) + 1

    return outcomes_count, joined_count, unknown_count, continuity_nulls


def _build_outcomes_analytics() -> Tuple[
    Dict[str, Dict[str, Any]],  # per-account analytics (truth)
    Dict[str, Any],             # global analytics
]:
    """
    Outcomes-derived truth analytics:
      - trades, pnl, winrate
      - top symbols/setups/timeframes/modes/close_reasons
      - LTM (last 24h) + bucket tier summaries + edge decay
    """
    cutover_ts = _get_cutover_ts_ms()
    rows = _safe_read_jsonl(OUTCOMES_V1)
    now_ms = _now_ms()
    cutoff_24h = now_ms - 24 * 3600 * 1000

    # config threshold for "bucket crossing"
    n_threshold = int(os.getenv("LTM_BUCKET_N_THRESHOLD", "30") or "30")

    per_acc: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
        "n": 0,
        "n_win": 0,
        "n_loss": 0,
        "pnl_sum": 0.0,
        "fees_sum": 0.0,
        "last_ts_ms": 0,
        "symbols": Counter(),
        "setups": Counter(),
        "timeframes": Counter(),
        "modes": Counter(),
        "close_reasons": Counter(),

        # LTM
        "n_24h": 0,
        "pnl_24h": 0.0,
        "unique_buckets_24h": set(),

        # bucket analytics
        "bucket_counts_total": Counter(),
        "bucket_counts_prev": Counter(),  # up to 24h cutoff
        "bucket_wins_total": Counter(),
        "bucket_last_trade_ts": {},       # key -> ts_ms
        "bucket_last_profit_ts": {},      # key -> ts_ms
    })

    global_stats = {
        "n": 0,
        "n_win": 0,
        "n_loss": 0,
        "pnl_sum": 0.0,
        "fees_sum": 0.0,
        "last_ts_ms": 0,
        "symbols": Counter(),
        "setups": Counter(),
        "timeframes": Counter(),
        "modes": Counter(),
        "close_reasons": Counter(),
    }

    for r in rows:
        if str(r.get("schema_version", "")).strip() != "outcome.v1":
            continue

        ts_ms = _extract_ts_ms_from_outcome(r) or 0
        if cutover_ts is not None and ts_ms and ts_ms < cutover_ts:
            # keep pre-cutover out of truth analytics by default (consistent with integrity maps)
            continue

        acc = _extract_account_label_from_row(r) or "unknown"
        if not _is_valid_account_label(acc):
            # don't let weird labels pollute truth (they can exist historically)
            continue

        sym = _extract_symbol_from_row(r)
        setup = _extract_setup_type_from_row(r)
        tf = _extract_timeframe_from_row(r)
        mode = str(r.get("mode") or "unknown").strip() or "unknown"
        close_reason = str(r.get("close_reason") or "unknown").strip() or "unknown"

        pnl = _safe_float(r.get("pnl_usd"))
        fees = _safe_float(r.get("fees_usd"))

        rec = per_acc[acc]
        rec["n"] += 1
        global_stats["n"] += 1

        rec["pnl_sum"] += pnl
        rec["fees_sum"] += fees
        global_stats["pnl_sum"] += pnl
        global_stats["fees_sum"] += fees

        if pnl > 0:
            rec["n_win"] += 1
            rec["bucket_wins_total"][_bucket_key(sym, tf, setup)] += 1
            global_stats["n_win"] += 1
        elif pnl < 0:
            rec["n_loss"] += 1
            global_stats["n_loss"] += 1

        rec["symbols"][sym] += 1
        rec["setups"][setup] += 1
        rec["timeframes"][tf] += 1
        rec["modes"][mode] += 1
        rec["close_reasons"][close_reason] += 1

        global_stats["symbols"][sym] += 1
        global_stats["setups"][setup] += 1
        global_stats["timeframes"][tf] += 1
        global_stats["modes"][mode] += 1
        global_stats["close_reasons"][close_reason] += 1

        bkey = _bucket_key(sym, tf, setup)
        rec["bucket_counts_total"][bkey] += 1
        if ts_ms and ts_ms <= cutoff_24h:
            rec["bucket_counts_prev"][bkey] += 1

        # last trade time
        if ts_ms:
            last = rec["bucket_last_trade_ts"].get(bkey, 0)
            if ts_ms > last:
                rec["bucket_last_trade_ts"][bkey] = ts_ms

        # last profit time
        if ts_ms and pnl > 0:
            lastp = rec["bucket_last_profit_ts"].get(bkey, 0)
            if ts_ms > lastp:
                rec["bucket_last_profit_ts"][bkey] = ts_ms

        # LTM window
        if ts_ms and ts_ms >= cutoff_24h:
            rec["n_24h"] += 1
            rec["pnl_24h"] += pnl
            rec["unique_buckets_24h"].add(bkey)

        if ts_ms and ts_ms > int(rec["last_ts_ms"]):
            rec["last_ts_ms"] = ts_ms
        if ts_ms and ts_ms > int(global_stats["last_ts_ms"]):
            global_stats["last_ts_ms"] = ts_ms

    def finalize_account(rec: Dict[str, Any]) -> Dict[str, Any]:
        n = int(rec["n"])
        n_win = int(rec["n_win"])
        n_loss = int(rec["n_loss"])
        winrate = (n_win / n) if n > 0 else 0.0

        pnl_sum = float(rec["pnl_sum"])
        fees_sum = float(rec["fees_sum"])
        avg_pnl = (pnl_sum / n) if n > 0 else 0.0

        # LTM
        n_24h = int(rec["n_24h"])
        pnl_24h = float(rec["pnl_24h"])
        unique_buckets_24h = len(rec["unique_buckets_24h"])

        # buckets crossing threshold in last 24h (prev < N and now >= N)
        crossings = 0
        for k, total in rec["bucket_counts_total"].items():
            prev = int(rec["bucket_counts_prev"].get(k, 0))
            if prev < n_threshold <= int(total):
                crossings += 1

        # bucket tiers + decay
        tier_counts: Dict[str, int] = defaultdict(int)
        stale_buckets = 0

        bucket_last_trade_ts: Dict[str, int] = rec["bucket_last_trade_ts"]
        bucket_last_profit_ts: Dict[str, int] = rec["bucket_last_profit_ts"]

        now_ms = _now_ms()

        for bkey, total in rec["bucket_counts_total"].items():
            wins = int(rec["bucket_wins_total"].get(bkey, 0))
            wr = (wins / int(total)) if int(total) > 0 else 0.0

            lt = int(bucket_last_trade_ts.get(bkey, 0) or 0)
            lp = int(bucket_last_profit_ts.get(bkey, 0) or 0)

            last_trade_age_sec = ((now_ms - lt) / 1000.0) if lt else None
            last_profit_age_sec = ((now_ms - lp) / 1000.0) if lp else None

            tier = _bucket_tier(int(total), float(wr), last_trade_age_sec, last_profit_age_sec)
            tier_counts[tier] += 1

            if tier == "DEPRECATED":
                stale_buckets += 1

        # top buckets by n
        top_buckets = rec["bucket_counts_total"].most_common(10)

        return {
            "n": n,
            "n_win": n_win,
            "n_loss": n_loss,
            "winrate": round(winrate, 6),
            "pnl_sum": round(pnl_sum, 8),
            "fees_sum": round(fees_sum, 8),
            "avg_pnl": round(avg_pnl, 8),
            "last_ts_ms": int(rec["last_ts_ms"]),
            "last_ts_iso": _ms_to_iso(int(rec["last_ts_ms"])),

            # top dimensions
            "top_symbols": rec["symbols"].most_common(10),
            "top_setups": rec["setups"].most_common(10),
            "top_timeframes": rec["timeframes"].most_common(10),
            "top_modes": rec["modes"].most_common(10),
            "top_close_reasons": rec["close_reasons"].most_common(10),

            # LTM
            "ltm_trades_24h": n_24h,
            "ltm_pnl_24h": round(pnl_24h, 8),
            "ltm_unique_buckets_24h": unique_buckets_24h,
            "ltm_buckets_crossing_n_threshold_24h": crossings,
            "ltm_n_threshold": n_threshold,

            # bucket ladder + decay
            "bucket_tiers": dict(tier_counts),
            "bucket_stale_count": stale_buckets,
            "top_buckets": top_buckets,
        }

    accounts_out: Dict[str, Any] = {}
    for acc, agg in per_acc.items():
        accounts_out[acc] = finalize_account(agg)

    global_out = {
        "n": int(global_stats["n"]),
        "n_win": int(global_stats["n_win"]),
        "n_loss": int(global_stats["n_loss"]),
        "winrate": round((global_stats["n_win"] / global_stats["n"]) if global_stats["n"] else 0.0, 6),
        "pnl_sum": round(float(global_stats["pnl_sum"]), 8),
        "fees_sum": round(float(global_stats["fees_sum"]), 8),
        "last_ts_ms": int(global_stats["last_ts_ms"]),
        "last_ts_iso": _ms_to_iso(int(global_stats["last_ts_ms"])),
        "top_symbols": global_stats["symbols"].most_common(10),
        "top_setups": global_stats["setups"].most_common(10),
        "top_timeframes": global_stats["timeframes"].most_common(10),
        "top_modes": global_stats["modes"].most_common(10),
        "top_close_reasons": global_stats["close_reasons"].most_common(10),
    }

    return accounts_out, global_out


def hydrate_dashboard_rows() -> List[Dict[str, Any]]:
    """
    Canonical dashboard row hydrator.
    READ-ONLY. SAFE FOR LIVE.

    Truth sources:
      - state/orchestrator_state.json  (liveness + intended state)
      - state/ops_snapshot.json        (legacy performance fields, if present)
      - state/dashboard_snapshot.json  (optional, outcomes-based)
      - config/fleet_manifest.yaml     (config truth)
      - outcomes/trainable rebuilt     (integrity overlays)
      - outcomes.v1.jsonl              (truth analytics + LTM + buckets)
      - paper ledger files             (closed trades count)
    """
    orch = _safe_read_json(ORCH_STATE)
    ops = _safe_read_json(OPS_SNAPSHOT)
    dash_snap = _safe_read_json(DASHBOARD_SNAPSHOT)

    manifest_idx = _load_manifest_index()
    outcomes_count_map, joined_count_map, unknown_count_map, continuity_nulls_map = _build_integrity_maps()

    # orchestrator can be "subaccounts" or "procs"
    orch_subs = orch.get("subaccounts", None)
    orch_procs = orch.get("procs", None)

    subaccounts: Dict[str, Dict[str, Any]] = {}
    procs: Dict[str, Dict[str, Any]] = {}

    if isinstance(orch_subs, dict):
        for k, v in orch_subs.items():
            nk = _norm_account_label(k)
            if nk and _is_valid_account_label(nk):
                subaccounts[nk] = v if isinstance(v, dict) else {}
    if isinstance(orch_procs, dict):
        for k, v in orch_procs.items():
            nk = _norm_account_label(k)
            if nk and _is_valid_account_label(nk):
                procs[nk] = v if isinstance(v, dict) else {}

    ops_accounts = _normalize_ops_accounts(ops)

    # outcomes truth analytics (do NOT let this expand display set)
    outcomes_truth_accounts, outcomes_truth_global = _build_outcomes_analytics()

    # optional dashboard_snapshot.json (outcomes-based truth)
    dash_accounts = dash_snap.get("accounts", {}) if isinstance(dash_snap, dict) else {}
    dash_accounts_norm: Dict[str, Dict[str, Any]] = {}
    if isinstance(dash_accounts, dict):
        for k, v in dash_accounts.items():
            nk = _norm_account_label(k)
            if nk and _is_valid_account_label(nk):
                dash_accounts_norm[nk] = v if isinstance(v, dict) else {}

    # ------------------------------------------------------------
    # ROWSET HARDENING (the whole point)
    #
    # We only DISPLAY:
    #   - expected accounts (main + flashback01..flashback10)
    #   - + anything explicitly in manifest
    #   - + anything explicitly in orchestrator
    #
    # We DO NOT allow ops/outcomes/integrity/dash_snapshot to invent accounts.
    # Those sources are overlays only.
    # ------------------------------------------------------------
    expected = _expected_accounts()

    all_ids = set()
    all_ids.update(expected)
    all_ids.update([k for k in manifest_idx.keys() if _is_valid_account_label(k)])
    all_ids.update([k for k in subaccounts.keys() if _is_valid_account_label(k)])
    all_ids.update([k for k in procs.keys() if _is_valid_account_label(k)])

    # EXTRA GUARD: never allow junk keys to appear as accounts
    all_ids.difference_update({"components", "version", "updatedms", "updated_ms", "meta", "schema_version", "accounts", "global", "source"})

    rows: List[Dict[str, Any]] = []
    now_ms = _now_ms()

    for account_id in sorted(all_ids):
        acct = subaccounts.get(account_id, {})
        proc = procs.get(account_id, {})
        ops_acct = ops_accounts.get(account_id, {})
        man = manifest_idx.get(account_id, {})
        truth = outcomes_truth_accounts.get(account_id, {})
        ds = dash_accounts_norm.get(account_id, {})

        # ----------------------------
        # Lifecycle / liveness
        # ----------------------------
        alive = bool(proc.get("alive", False)) if isinstance(proc, dict) else False
        hb_proc = _pick_proc_heartbeat_ms(proc) if isinstance(proc, dict) else 0

        enabled_orch = bool(acct.get("enabled", False)) if isinstance(acct, dict) else False
        online_orch = bool(acct.get("online", False)) if isinstance(acct, dict) else False
        hb_orch = _safe_int(acct.get("last_heartbeat_ms", 0)) if isinstance(acct, dict) else 0

        online = alive if proc else online_orch
        heartbeat = hb_proc if hb_proc > 0 else hb_orch
        if heartbeat <= 0:
            # fallback to ops timestamp if present
            heartbeat = _safe_int(ops_acct.get("ts_ms", 0))
        if heartbeat <= 0:
            # fallback to truth last_ts
            heartbeat = _safe_int(truth.get("last_ts_ms", 0))

        # ----------------------------
        # Manifest overlay (truth)
        # ----------------------------
        enabled = bool(man.get("enabled")) if isinstance(man, dict) and ("enabled" in man) else enabled_orch
        automation_mode = str(man.get("automation_mode") or "").strip() if isinstance(man, dict) else ""
        ai_profile = str(man.get("ai_profile") or "").strip() if isinstance(man, dict) else ""
        role = man.get("role") if isinstance(man, dict) else None
        strategy_name = str(man.get("strategy_name") or man.get("strategy") or "unknown") if isinstance(man, dict) else "unknown"
        strategy_version = str(man.get("strategy_version") or "unknown") if isinstance(man, dict) else "unknown"

        # ----------------------------
        # Integrity overlays
        # ----------------------------
        outcomes_count = int(outcomes_count_map.get(account_id, 0))
        joined_count = int(joined_count_map.get(account_id, 0))
        unknown_count = int(unknown_count_map.get(account_id, 0))
        continuity_nulls = int(continuity_nulls_map.get(account_id, 0))

        join_pct = (joined_count / outcomes_count * 100.0) if outcomes_count > 0 else 0.0
        unknown_pct = (unknown_count / outcomes_count * 100.0) if outcomes_count > 0 else 0.0
        orphans_count = max(outcomes_count - joined_count, 0)

        paper_closed_trades = _count_paper_closed(account_id)
        outcomes_delta = int(paper_closed_trades - outcomes_count)

        integrity_score = _calc_integrity_score(
            join_pct=join_pct,
            unknown_pct=unknown_pct,
            continuity_nulls=continuity_nulls,
            orphans_count=orphans_count,
            outcomes_delta=outcomes_delta,
        )
        integrity_alerts = _calc_integrity_alerts(
            join_pct=join_pct,
            unknown_pct=unknown_pct,
            continuity_nulls=continuity_nulls,
            orphans_count=orphans_count,
            outcomes_delta=outcomes_delta,
        )

        status = _classify_status(enabled=enabled, online=online, heartbeat_ms=heartbeat)
        state_color = _status_color(status, integrity_score, integrity_alerts)

        # ----------------------------
        # Truth analytics (outcomes-derived)
        # ----------------------------
        # Prefer outcomes-truth stats; fall back to dashboard_snapshot; then ops_snapshot.
        truth_n = _safe_int(truth.get("n", 0))
        truth_winrate = float(truth.get("winrate", 0.0) or 0.0)
        truth_pnl = float(truth.get("pnl_sum", 0.0) or 0.0)
        truth_n_win = _safe_int(truth.get("n_win", 0))
        truth_n_loss = _safe_int(truth.get("n_loss", 0))
        truth_last_ts = _safe_int(truth.get("last_ts_ms", 0))

        # ds schema uses n/winrate/pnl_sum too
        ds_n = _safe_int(ds.get("n", 0))
        ds_winrate = float(ds.get("winrate", 0.0) or 0.0)
        ds_pnl = float(ds.get("pnl_sum", 0.0) or 0.0)
        ds_last_ts = _safe_int(ds.get("last_ts_ms", 0))

        # ops legacy fields
        ops_trades_closed = _safe_int(ops_acct.get("trades_closed", 0))
        ops_pnl = float(ops_acct.get("pnl", 0.0) or 0.0)
        ops_ai_conf = float(ops_acct.get("ai_confidence", 0.0) or 0.0)
        ops_risk_level = str(ops_acct.get("risk_level", "unknown") or "unknown")
        ops_ts = _safe_int(ops_acct.get("ts_ms", 0))

        # pick truth source
        if truth_n > 0:
            total_trades = truth_n
            win_rate_pct = round(truth_winrate * 100.0, 2)
            cumulative_return = truth_pnl
            win_count = truth_n_win
            loss_count = truth_n_loss
            truth_source = "outcomes_v1"
            truth_last_ts_ms = truth_last_ts or heartbeat
        elif ds_n > 0:
            total_trades = ds_n
            win_rate_pct = round(ds_winrate * 100.0, 2)
            cumulative_return = ds_pnl
            win_count = 0
            loss_count = 0
            truth_source = "dashboard_snapshot"
            truth_last_ts_ms = ds_last_ts or heartbeat
        elif ops_trades_closed > 0:
            total_trades = ops_trades_closed
            win_rate_pct = 0.0  # ops doesn't have wins/losses in your current schema
            cumulative_return = ops_pnl
            win_count = 0
            loss_count = 0
            truth_source = "ops_snapshot"
            truth_last_ts_ms = ops_ts or heartbeat
        else:
            total_trades = 0
            win_rate_pct = 0.0
            cumulative_return = 0.0
            win_count = 0
            loss_count = 0
            truth_source = "none"
            truth_last_ts_ms = heartbeat or 0

        # top chips from truth/ds (lists of [name,count])
        top_symbols = truth.get("top_symbols") or ds.get("top_symbols") or []
        top_setups = truth.get("top_setups") or ds.get("top_setups") or []
        top_timeframes = truth.get("top_timeframes") or ds.get("top_timeframes") or []
        top_modes = truth.get("top_modes") or ds.get("top_modes") or []
        top_close_reasons = truth.get("top_close_reasons") or ds.get("top_close_reasons") or []

        # LTM + ladder + decay from truth
        ltm_trades_24h = _safe_int(truth.get("ltm_trades_24h", 0))
        ltm_pnl_24h = float(truth.get("ltm_pnl_24h", 0.0) or 0.0)
        ltm_unique_buckets_24h = _safe_int(truth.get("ltm_unique_buckets_24h", 0))
        ltm_crossings = _safe_int(truth.get("ltm_buckets_crossing_n_threshold_24h", 0))
        ltm_n_threshold = _safe_int(truth.get("ltm_n_threshold", 0))

        bucket_tiers = truth.get("bucket_tiers") or {}
        if not isinstance(bucket_tiers, dict):
            bucket_tiers = {}
        bucket_stale_count = _safe_int(truth.get("bucket_stale_count", 0))
        top_buckets = truth.get("top_buckets") or []
        if not isinstance(top_buckets, list):
            top_buckets = []

        # role enforcement
        role_ok, role_note = _role_enforcement(str(role or ""), {str(k): _safe_int(v) for k, v in bucket_tiers.items()})

        # cull recommendation
        cull, cull_reason = _cull_recommendation(
            total_trades=int(total_trades),
            winrate=float((win_rate_pct / 100.0) if win_rate_pct else 0.0),
            integrity_score=int(integrity_score),
            integrity_alerts=list(integrity_alerts),
        )

        # readiness for full automation (dashboard-level)
        # This does NOT mean "deploy live", it means "data integrity + stability OK".
        min_ready_trades = int(os.getenv("READY_MIN_TRADES", "50") or "50")
        min_ready_integrity = int(os.getenv("READY_MIN_INTEGRITY", "90") or "90")
        ready = True
        ready_reasons: List[str] = []
        if not enabled:
            ready = False
            ready_reasons.append("disabled_in_manifest_or_orch")
        if status != "ONLINE":
            ready = False
            ready_reasons.append(f"status_not_online:{status}")
        if integrity_score < min_ready_integrity:
            ready = False
            ready_reasons.append(f"integrity_score_low:{integrity_score}")
        if integrity_alerts:
            ready = False
            ready_reasons.append(f"integrity_alerts:{','.join(integrity_alerts)}")
        if total_trades < min_ready_trades:
            ready = False
            ready_reasons.append(f"insufficient_trades:{total_trades}<{min_ready_trades}")

        # N buckets (best effort)
        # If you later add a real bucket scoreboard store, swap this source.
        n_buckets = None
        n_buckets_source = "none"
        if isinstance(ops_acct, dict) and "buckets" in ops_acct:
            n_buckets = _safe_int(ops_acct.get("buckets", 0))
            n_buckets_source = "ops_snapshot"
        elif isinstance(truth, dict) and isinstance(bucket_tiers, dict):
            # approximated: number of observed buckets
            n_buckets = sum(_safe_int(v) for v in bucket_tiers.values())
            n_buckets_source = "outcomes_v1_approx"

        # Telegram wiring (do NOT expose tokens; expose whether notifier is configured)
        telegram_configured = bool(man.get("telegram_enabled")) if isinstance(man, dict) and ("telegram_enabled" in man) else None

        # Balance fields (truthful: only show if present; else 0.0)
        total_balance = _safe_float(ops_acct.get("total_balance", ops_acct.get("equity", 0.0)))
        available_balance = _safe_float(ops_acct.get("available_balance", 0.0))

        row = {
            # Identity
            "account_label": account_id,
            "strategy_name": strategy_name,
            "strategy_version": strategy_version,

            # Manifest overlay (truth)
            "automation_mode": automation_mode or "unknown",
            "manifest_enabled": bool(man.get("enabled")) if isinstance(man, dict) and ("enabled" in man) else None,
            "enable_ai_stack": bool(man.get("enable_ai_stack")) if isinstance(man, dict) and ("enable_ai_stack" in man) else None,
            "role": role,
            "ai_profile": ai_profile or None,
            "risk_pct": (man.get("risk_pct") if isinstance(man, dict) else None),

            # Lifecycle
            "enabled": enabled,
            "online": online,
            "heartbeat": heartbeat,
            "status": status,
            "state_color": state_color,

            # truth timestamps (used for UI freshness)
            "truth_last_ts_ms": int(truth_last_ts_ms or 0),
            "freshness_sec": round(((now_ms - int(truth_last_ts_ms)) / 1000.0), 3) if truth_last_ts_ms else None,

            # Balance
            "total_balance": float(total_balance),
            "available_balance": float(available_balance),

            # Trade + performance (truth-first)
            "total_trades": int(total_trades),
            "win_count": int(win_count),
            "loss_count": int(loss_count),
            "win_rate_pct": float(win_rate_pct),
            "cumulative_pnl_usd": float(cumulative_return),
            "truth_source": truth_source,

            # Ops-only fields (optional, fail-soft)
            "risk_state": ops_risk_level,
            "confidence_score": ops_ai_conf,

            # N buckets (must be visible)
            "n_buckets": n_buckets,
            "n_buckets_source": n_buckets_source,

            # Pipeline Integrity
            "outcomes_count": outcomes_count,
            "joined_count": joined_count,
            "join_pct": round(join_pct, 2),
            "unknown_count": unknown_count,
            "unknown_pct": round(unknown_pct, 2),
            "orphans_count": orphans_count,
            "continuity_nulls": continuity_nulls,
            "paper_closed_trades": paper_closed_trades,
            "outcomes_delta": outcomes_delta,

            # Integrity scoring/alerts
            "integrity_score": int(integrity_score),
            "integrity_alerts": list(integrity_alerts),

            # LTM (Learning Throughput Metrics)
            "ltm_trades_24h": ltm_trades_24h,
            "ltm_pnl_24h": ltm_pnl_24h,
            "ltm_unique_buckets_24h": ltm_unique_buckets_24h,
            "ltm_buckets_crossing_n_threshold_24h": ltm_crossings,
            "ltm_n_threshold": ltm_n_threshold,

            # Bucket ladder + edge decay (truth-based)
            "bucket_tiers": bucket_tiers,
            "bucket_stale_count": bucket_stale_count,
            "top_buckets": top_buckets,

            # Role enforcement + cull recommendation
            "role_ok": bool(role_ok),
            "role_note": role_note,
            "cull_recommended": bool(cull),
            "cull_reason": cull_reason,

            # Full automation readiness
            "ready_for_full_auto": bool(ready),
            "ready_reasons": ready_reasons,

            # Top chips for UI
            "top_symbols": top_symbols if isinstance(top_symbols, list) else [],
            "top_setups": top_setups if isinstance(top_setups, list) else [],
            "top_timeframes": top_timeframes if isinstance(top_timeframes, list) else [],
            "top_modes": top_modes if isinstance(top_modes, list) else [],
            "top_close_reasons": top_close_reasons if isinstance(top_close_reasons, list) else [],

            # Telegram (health only, not tokens)
            "telegram_configured": telegram_configured,

            # Placeholders for future enhancements (truthful)
            "counterfactual_ready": False,
            "counterfactual_note": "not_implemented",
            "meta_allocator_ready": False,
            "meta_allocator_note": "not_in_scope_yet",

            # Metadata
            "last_updated_ms": now_ms,
            "schema_version": SCHEMA_VERSION,
        }

        # ------------------------------------------------------------
        # TEMPLATE COMPATIBILITY ALIASES (prevents Jinja crashes)
        # Your template used r.pnl_pct and died. So we provide it.
        # ------------------------------------------------------------
        # Used for pos/neg styling. We provide cumulative pnl as the numeric.
        row["pnl_pct"] = float(row.get("cumulative_pnl_usd", 0.0))
        row["pnl_usd"] = float(row.get("cumulative_pnl_usd", 0.0))
        row["win_pct"] = float(row.get("win_rate_pct", 0.0))
        row["winrate"] = float(row.get("win_rate_pct", 0.0))
        row["trades_total"] = int(row.get("total_trades", 0))
        row["is_online"] = bool(row.get("online", False))

        # Some templates expect open_trade
        row["open_trade"] = False  # until you wire live open positions into ops snapshot

        # Some templates expect avg/cumulative return naming
        row["avg_return_pct"] = 0.0
        row["cumulative_return_pct"] = float(row.get("cumulative_pnl_usd", 0.0))

        rows.append(row)

    return rows
