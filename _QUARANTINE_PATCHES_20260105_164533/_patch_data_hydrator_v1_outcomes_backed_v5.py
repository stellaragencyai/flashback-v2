from __future__ import annotations

from pathlib import Path
import re

TARGET = Path(r"app\dashboard\data_hydrator_v1.py")

INSERT = """
# ---------------------------------------------------------------------------
# Outcomes-backed trade stats (canonical truth)
# ---------------------------------------------------------------------------

OUTCOMES_V1 = STATE_DIR / "ai_events" / "outcomes.v1.jsonl"

def _iter_jsonl(path: Path):
    if not path.exists():
        return
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = (line or "").strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                if isinstance(obj, dict):
                    yield obj
    except Exception:
        return

def _load_outcomes_stats() -> Dict[str, Dict[str, Any]]:
    \"""
    Returns dict keyed by account_label:
      {
        "total": int,
        "wins": int,
        "losses": int,
        "win_rate_pct": float,
        "pnl_total_usd": float,
        "pnl_avg_usd": float,
        "last_outcome_ts_ms": int
      }
    \"""
    stats: Dict[str, Dict[str, Any]] = {}

    for row in _iter_jsonl(OUTCOMES_V1):
        if row.get("schema_version") != "outcome.v1":
            continue

        acct = row.get("account_label")
        if not isinstance(acct, str) or not acct.strip():
            continue
        acct = acct.strip()

        try:
            pnl_f = float(row.get("pnl_usd") or 0.0)
        except Exception:
            pnl_f = 0.0

        ts = row.get("ts_ms") or row.get("closed_ts_ms") or 0
        try:
            ts_i = int(ts)
        except Exception:
            ts_i = 0

        s = stats.get(acct)
        if s is None:
            s = {
                "total": 0,
                "wins": 0,
                "losses": 0,
                "pnl_total_usd": 0.0,
                "last_outcome_ts_ms": 0,
            }
            stats[acct] = s

        s["total"] += 1
        if pnl_f > 0:
            s["wins"] += 1
        elif pnl_f < 0:
            s["losses"] += 1

        s["pnl_total_usd"] += pnl_f
        if ts_i > int(s.get("last_outcome_ts_ms", 0) or 0):
            s["last_outcome_ts_ms"] = ts_i

    for acct, s in stats.items():
        t = int(s.get("total", 0) or 0)
        w = int(s.get("wins", 0) or 0)
        pnl_total = float(s.get("pnl_total_usd", 0.0) or 0.0)
        s["win_rate_pct"] = round((w / t * 100.0), 2) if t > 0 else 0.0
        s["pnl_avg_usd"] = (pnl_total / t) if t > 0 else 0.0

    return stats
"""

def main():
    if not TARGET.exists():
        raise SystemExit(f"STOP: missing {TARGET}")

    src = TARGET.read_text(encoding="utf-8", errors="ignore")

    if 'OUTCOMES_V1 = STATE_DIR / "ai_events" / "outcomes.v1.jsonl"' in src:
        print("OK: data_hydrator_v1 already patched (outcomes-backed stats present).")
        return

    lines = src.splitlines(True)

    # Find def _now_ms line
    idx_def = None
    for i, line in enumerate(lines):
        if line.lstrip().startswith("def _now_ms"):
            idx_def = i
            break
    if idx_def is None:
        raise SystemExit("STOP: could not find def _now_ms anchor")

    # Find insertion point = first blank line AFTER the _now_ms block (we expect a blank line)
    insert_at = None
    for j in range(idx_def + 1, min(idx_def + 40, len(lines))):
        if lines[j].strip() == "":
            insert_at = j + 1
            break
    if insert_at is None:
        # fallback: insert after idx_def+3
        insert_at = min(idx_def + 4, len(lines))

    out_lines = lines[:insert_at] + [INSERT + "\n"] + lines[insert_at:]
    out = "".join(out_lines)

    # Wire outcomes_stats load
    needle_ops = '    ops_accounts = ops.get("accounts", {})\n'
    if needle_ops not in out:
        # tolerate different spacing
        if 'ops_accounts = ops.get("accounts", {})' not in out:
            raise SystemExit("STOP: could not find ops_accounts line to wire outcomes_stats")
        out = out.replace('ops_accounts = ops.get("accounts", {})', 'ops_accounts = ops.get("accounts", {})\n    outcomes_stats = _load_outcomes_stats()')
    else:
        out = out.replace(
            needle_ops,
            '    ops_accounts = ops.get("accounts", {})\n    outcomes_stats = _load_outcomes_stats()\n'
        )

    # Replace perf/trades/ai/risk extraction with injected block
    block_old = (
        '        trades = ops_acct.get("trades", {})\n'
        '        perf = ops_acct.get("performance", {})\n'
        '        ai = ops_acct.get("ai", {})\n'
        '        risk = ops_acct.get("risk", {})\n'
    )
    block_new = (
        '        trades = ops_acct.get("trades", {})\n'
        '        perf = ops_acct.get("performance", {})\n'
        '        ai = ops_acct.get("ai", {})\n'
        '        risk = ops_acct.get("risk", {})\n'
        '\n'
        '        # Prefer canonical outcomes-derived stats when available\n'
        '        o = outcomes_stats.get(account_id) or outcomes_stats.get(acct.get("label", account_id)) or {}\n'
        '        o_total = int(o.get("total", 0) or 0)\n'
        '        o_wins = int(o.get("wins", 0) or 0)\n'
        '        o_losses = int(o.get("losses", 0) or 0)\n'
        '        o_win_rate = float(o.get("win_rate_pct", 0.0) or 0.0)\n'
        '        o_pnl_total = float(o.get("pnl_total_usd", 0.0) or 0.0)\n'
        '        o_pnl_avg = float(o.get("pnl_avg_usd", 0.0) or 0.0)\n'
        '        o_last_ts = int(o.get("last_outcome_ts_ms", 0) or 0)\n'
    )
    if block_old not in out:
        raise SystemExit("STOP: could not find trades/perf/ai/risk block to replace")
    out = out.replace(block_old, block_new)

    # Override total_trades/wins/losses/win_rate
    out = re.sub(
        r'(?ms)^\s*total_trades = int\(trades\.get\("total", 0\)\)\s*\n\s*wins = int\(trades\.get\("wins", 0\)\)\s*\n\s*losses = int\(trades\.get\("losses", 0\)\)\s*\n\s*\n\s*win_rate = \(wins / total_trades \* 100\.0\) if total_trades > 0 else 0\.0\s*\n',
        (
            '        total_trades = o_total if o_total > 0 else int(trades.get("total", 0))\n'
            '        wins = o_wins if o_total > 0 else int(trades.get("wins", 0))\n'
            '        losses = o_losses if o_total > 0 else int(trades.get("losses", 0))\n'
            '\n'
            '        win_rate = o_win_rate if o_total > 0 else ((wins / total_trades * 100.0) if total_trades > 0 else 0.0)\n'
        ),
        out
    )

    # Inject PnL fields after win_rate_pct
    anchor = '            "win_rate_pct": round(win_rate, 2),\n'
    if anchor not in out:
        raise SystemExit("STOP: could not find win_rate_pct field to inject pnl fields")
    out = out.replace(
        anchor,
        anchor +
        '\n'
        '            # Truthful USD performance (from outcomes.v1)\n'
        '            "pnl_total_usd": float(o_pnl_total),\n'
        '            "pnl_avg_usd": float(o_pnl_avg),\n'
        '            "last_outcome_ts_ms": int(o_last_ts),\n'
    )

    TARGET.write_text(out, encoding="utf-8")
    print("OK: patched data_hydrator_v1.py (outcomes-backed trade stats + pnl fields).")

if __name__ == "__main__":
    main()
