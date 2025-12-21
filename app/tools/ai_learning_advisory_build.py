#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Learning Advisory Build Tool v1.1 (Phase 6)

Reads (read-only):
- state/ai_learning/learning.sqlite (table: memory_stats_v1)

Writes (derived, rebuildable, deterministic ordering):
- state/ai_learning/advisory_v1.jsonl
- state/ai_learning/advisory_rankings_v1.json

Rules:
- No writes to Phase 5 memory. Ever.
- Conservative gating: insufficient data => NEUTRAL
- Rankings report BOTH:
    • global counts (all rows)
    • eligible counts (Phase 7-consumable)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from typing import Any, Dict, List, Optional, Tuple

from app.ai.ai_learning_contract import LearningPaths, ADVISORY_SCHEMA_VERSION
from app.ai.ai_learning_advisor import AdvisoryRow, recommend, row_to_dict


def _exists_nonempty(p) -> bool:
    try:
        return p.exists() and p.stat().st_size > 0
    except Exception:
        return False


def _connect_ro(db_path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _stable_key(policy_hash: str, mfp: str, symbol: str, timeframe: str, setup_type: str) -> Tuple[str, str, str, str, str]:
    return (policy_hash or "", mfp or "", symbol or "", timeframe or "", setup_type or "")


def _eligibility(
    r: AdvisoryRow,
    *,
    min_trades: int,
) -> Tuple[str, bool]:
    """
    Phase-6 eligibility for rankings/consumption.
    Return (eligibility_state, eligible_bool).
    """
    if r.confidence_state == "INSUFFICIENT_DATA":
        return ("INSUFFICIENT_DATA", False)
    if int(r.sample_size) < int(min_trades):
        return ("BELOW_MIN_TRADES", False)
    return ("ELIGIBLE", True)


def build_advisory_rows(
    *,
    policy_hash_filter: Optional[str],
    favor_threshold: float,
    avoid_threshold: float,
) -> Tuple[List[AdvisoryRow], Dict[str, Any]]:
    lp = LearningPaths.default()
    if not _exists_nonempty(lp.learning_sqlite_path):
        return ([], {"ok": False, "reason": f"missing_or_empty_learning_sqlite: {lp.learning_sqlite_path}"})

    conn = _connect_ro(lp.learning_sqlite_path)
    cur = conn.cursor()

    where = ["1=1"]
    params: Dict[str, Any] = {}
    if policy_hash_filter:
        where.append("policy_hash = :ph")
        params["ph"] = policy_hash_filter

    # built_ts_ms is deterministic already inside learning rows
    cur.execute(f"SELECT MAX(built_ts_ms) AS m FROM memory_stats_v1 WHERE {' AND '.join(where)};", params)
    built_ts_ms = int(cur.fetchone()["m"] or 0)

    cur.execute(
        f"""
        SELECT
          policy_hash, memory_fingerprint, symbol, timeframe, setup_type,
          n, win_rate,
          mean_r, median_r, mad_r, winsor_mean_r,
          confidence_state, confidence,
          drift_flag, drift_reason,
          last_ts_ms
        FROM memory_stats_v1
        WHERE {' AND '.join(where)}
        ORDER BY policy_hash ASC, memory_fingerprint ASC, symbol ASC, timeframe ASC, setup_type ASC;
        """,
        params,
    )

    rows: List[AdvisoryRow] = []
    fetched = cur.fetchall()
    for r in fetched:
        n = int(r["n"] or 0)
        conf_state = str(r["confidence_state"] or "INSUFFICIENT_DATA")
        conf = float(r["confidence"] or 0.0)

        winsor = r["winsor_mean_r"]
        mean_r = r["mean_r"]
        win_rate = r["win_rate"]

        drift_flag = bool(int(r["drift_flag"] or 0))
        drift_reason = str(r["drift_reason"] or "")

        rec, reasons = recommend(
            n=n,
            confidence_state=conf_state,
            confidence=conf,
            winsor_mean_r=(float(winsor) if winsor is not None else None),
            expected_r=(float(mean_r) if mean_r is not None else None),
            win_rate=(float(win_rate) if win_rate is not None else None),
            drift_flag=drift_flag,
            drift_reason=drift_reason,
            favor_threshold=float(favor_threshold),
            avoid_threshold=float(avoid_threshold),
        )

        rows.append(
            AdvisoryRow(
                schema_version=ADVISORY_SCHEMA_VERSION,
                built_ts_ms=built_ts_ms,
                policy_hash=str(r["policy_hash"] or ""),
                memory_fingerprint=str(r["memory_fingerprint"] or ""),
                symbol=str(r["symbol"] or ""),
                timeframe=str(r["timeframe"] or ""),
                setup_type=str(r["setup_type"] or ""),
                sample_size=n,
                confidence_state=conf_state,
                confidence=conf,
                expected_r=(float(mean_r) if mean_r is not None else None),
                winsor_mean_r=(float(winsor) if winsor is not None else None),
                median_r=(float(r["median_r"]) if r["median_r"] is not None else None),
                mad_r=(float(r["mad_r"]) if r["mad_r"] is not None else None),
                win_rate=(float(win_rate) if win_rate is not None else None),
                drift_flag=drift_flag,
                drift_reason=drift_reason,
                recommendation=rec,
                reasons=reasons,
            )
        )

    conn.close()

    meta = {
        "ok": True,
        "built_ts_ms": built_ts_ms,
        "rows": len(rows),
        "policy_hash_filter": policy_hash_filter,
        "favor_threshold": float(favor_threshold),
        "avoid_threshold": float(avoid_threshold),
    }
    return (rows, meta)


def write_advisory_artifacts(
    *,
    advisory_jsonl_path,
    advisory_rankings_json_path,
    rows: List[AdvisoryRow],
    min_trades: int,
    include_insufficient_in_rankings: bool,
    max_per_bucket: int,
    built_ts_ms: int,
) -> Dict[str, Any]:
    # Fresh outputs for determinism
    for p in (advisory_jsonl_path, advisory_rankings_json_path):
        if p.exists():
            try:
                p.unlink()
            except Exception:
                pass

    advisory_jsonl_path.parent.mkdir(parents=True, exist_ok=True)

    # JSONL: deterministic ordering
    rows_sorted = sorted(
        rows,
        key=lambda x: _stable_key(x.policy_hash, x.memory_fingerprint, x.symbol, x.timeframe, x.setup_type),
    )

    # Write advisory JSONL with explicit eligibility fields (Phase 6 convenience)
    with advisory_jsonl_path.open("wb") as f:
        for r in rows_sorted:
            d = row_to_dict(r)
            elig_state, elig_bool = _eligibility(r, min_trades=int(min_trades))
            d["eligibility"] = elig_state
            d["eligible"] = bool(elig_bool)
            line = json.dumps(d, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
            f.write(line + b"\n")

    # -------------------
    # Rankings + counts
    # -------------------

    # Global counts (all rows, including insufficient)
    global_favor = [r for r in rows_sorted if r.recommendation == "FAVOR"]
    global_avoid = [r for r in rows_sorted if r.recommendation == "AVOID"]
    global_neutral = [r for r in rows_sorted if r.recommendation == "NEUTRAL"]
    global_insufficient = [r for r in rows_sorted if r.confidence_state == "INSUFFICIENT_DATA"]
    global_below_min = [r for r in rows_sorted if (r.confidence_state != "INSUFFICIENT_DATA" and int(r.sample_size) < int(min_trades))]

    # Eligibility function used for “eligible rankings”
    def eligible_for_rankings(r: AdvisoryRow) -> bool:
        if include_insufficient_in_rankings:
            return True
        elig_state, elig_bool = _eligibility(r, min_trades=int(min_trades))
        _ = elig_state
        return bool(elig_bool)

    elig = [r for r in rows_sorted if eligible_for_rankings(r)]

    favor = [r for r in elig if r.recommendation == "FAVOR"]
    avoid = [r for r in elig if r.recommendation == "AVOID"]
    neutral = [r for r in elig if r.recommendation == "NEUTRAL"]

    # Stable sorts
    favor.sort(key=lambda r: (-(r.winsor_mean_r or 0.0), -(r.confidence or 0.0), -r.sample_size))
    avoid.sort(key=lambda r: ((r.winsor_mean_r or 0.0), -(r.confidence or 0.0), -r.sample_size))
    neutral.sort(key=lambda r: (-(r.confidence or 0.0), -r.sample_size))

    def mini(r: AdvisoryRow) -> Dict[str, Any]:
        elig_state, elig_bool = _eligibility(r, min_trades=int(min_trades))
        return {
            "policy_hash": r.policy_hash,
            "memory_fingerprint": r.memory_fingerprint,
            "symbol": r.symbol,
            "timeframe": r.timeframe,
            "setup_type": r.setup_type,
            "n": r.sample_size,
            "confidence_state": r.confidence_state,
            "confidence": r.confidence,
            "winsor_mean_r": r.winsor_mean_r,
            "recommendation": r.recommendation,
            "eligibility": elig_state,
            "eligible": bool(elig_bool),
            "drift_flag": bool(r.drift_flag),
            "drift_reason": r.drift_reason,
        }

    rankings = {
        "schema_version": ADVISORY_SCHEMA_VERSION,
        "built_ts_ms": int(built_ts_ms),
        "min_trades": int(min_trades),
        "include_insufficient_in_rankings": bool(include_insufficient_in_rankings),
        "counts": {
            # Global reality
            "rows_total": len(rows_sorted),
            "global": {
                "favor": len(global_favor),
                "avoid": len(global_avoid),
                "neutral": len(global_neutral),
                "insufficient_data": len(global_insufficient),
                "below_min_trades": len(global_below_min),
            },
            # What Phase 7 would actually consume by default
            "eligible_only": {
                "rows_eligible": len(elig),
                "favor": len(favor),
                "avoid": len(avoid),
                "neutral": len(neutral),
            },
        },
        "top_favor": [mini(r) for r in favor[: int(max_per_bucket)]],
        "top_avoid": [mini(r) for r in avoid[: int(max_per_bucket)]],
        "top_neutral": [mini(r) for r in neutral[: int(max_per_bucket)]],
    }

    advisory_rankings_json_path.parent.mkdir(parents=True, exist_ok=True)
    advisory_rankings_json_path.write_text(json.dumps(rankings, indent=2, sort_keys=True), encoding="utf-8")

    b = advisory_jsonl_path.read_bytes()
    return {
        "ok": True,
        "advisory_jsonl": str(advisory_jsonl_path),
        "advisory_rankings_json": str(advisory_rankings_json_path),
        "advisory_bytes": len(b),
        "advisory_sha256": _sha256_bytes(b),
        "rankings_counts": rankings["counts"],
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rebuild", action="store_true", help="Rebuild advisory artifacts (default).")
    ap.add_argument("--policy-hash", type=str, default="", help="Optional filter to a single policy_hash.")
    ap.add_argument("--min-trades", type=int, default=30, help="Minimum trades for ranking eligibility (default 30).")
    ap.add_argument("--include-insufficient-in-rankings", action="store_true", help="Include insufficient rows in rankings.")
    ap.add_argument("--favor-threshold", type=float, default=0.10, help="winsor_mean_r >= this => FAVOR (default 0.10).")
    ap.add_argument("--avoid-threshold", type=float, default=-0.10, help="winsor_mean_r <= this => AVOID (default -0.10).")
    ap.add_argument("--max-per-bucket", type=int, default=50, help="Max rows per bucket in rankings (default 50).")
    args = ap.parse_args()

    policy_hash = args.policy_hash.strip() or None

    lp = LearningPaths.default()
    rows, meta = build_advisory_rows(
        policy_hash_filter=policy_hash,
        favor_threshold=float(args.favor_threshold),
        avoid_threshold=float(args.avoid_threshold),
    )

    print("=== AI Learning Advisory Build v1.1 (Phase 6) ===")
    if not meta.get("ok"):
        print("FAIL ❌", meta.get("reason"))
        return

    out = write_advisory_artifacts(
        advisory_jsonl_path=lp.advisory_jsonl_path,
        advisory_rankings_json_path=lp.advisory_rankings_json_path,
        rows=rows,
        min_trades=int(args.min_trades),
        include_insufficient_in_rankings=bool(args.include_insufficient_in_rankings),
        max_per_bucket=int(args.max_per_bucket),
        built_ts_ms=int(meta.get("built_ts_ms") or 0),
    )

    if not out.get("ok"):
        print("FAIL ❌ advisory_write_failed")
        return

    print("schema_version  :", ADVISORY_SCHEMA_VERSION)
    print("built_ts_ms     :", meta.get("built_ts_ms"))
    print("rows_total      :", meta.get("rows"))
    print("favor_threshold :", meta.get("favor_threshold"))
    print("avoid_threshold :", meta.get("avoid_threshold"))
    print("policy_filter   :", meta.get("policy_hash_filter") or "NONE")
    print("min_trades      :", int(args.min_trades))
    print("include_insuf   :", bool(args.include_insufficient_in_rankings))
    print("advisory_jsonl  :", out.get("advisory_jsonl"))
    print("advisory_bytes  :", out.get("advisory_bytes"))
    print("advisory_sha256 :", out.get("advisory_sha256"))
    print("rankings_json   :", out.get("advisory_rankings_json"))
    print("rankings_counts :", out.get("rankings_counts"))
    print("DONE ✅")


if __name__ == "__main__":
    main()
