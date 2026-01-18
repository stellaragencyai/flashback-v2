import json
from collections import defaultdict
from pathlib import Path

IN_PATH = Path("state/ai_events/outcomes.v1.trainable.jsonl")

def main():
    if not IN_PATH.exists():
        raise SystemExit(f"FAIL: missing {IN_PATH}")

    buckets = defaultdict(lambda: {"n": 0, "wins": 0, "pnl": 0.0})

    for ln in IN_PATH.read_text(encoding="utf-8", errors="ignore").splitlines():
        if not ln.strip():
            continue
        try:
            o = json.loads(ln)
        except Exception:
            continue

        acct = str(o.get("account_label") or "unknown")
        sym = str(o.get("symbol") or "unknown")
        tf = str(o.get("timeframe") or "unknown")
        st = str(o.get("setup_type") or "unknown")

        key = (acct, sym, tf, st)
        b = buckets[key]
        b["n"] += 1
        pnl = float(o.get("pnl_usd") or 0.0)
        b["pnl"] += pnl
        if pnl > 0:
            b["wins"] += 1

    rows = []
    for (acct, sym, tf, st), b in buckets.items():
        n = b["n"]
        wins = b["wins"]
        pnl = b["pnl"]
        winrate = (wins / n) if n else 0.0
        avg_pnl = (pnl / n) if n else 0.0
        rows.append((n, pnl, winrate, avg_pnl, acct, sym, tf, st))

    rows.sort(reverse=True)  # highest n first, then pnl, etc.

    print("=== SCOREBOARD_V0 (from outcomes.v1.trainable.jsonl) ===")
    print(f"rows={len(rows)} total_outcomes={sum(r[0] for r in rows)}")
    print("")
    print("N    PNL_USD   WINRATE  AVG_PNL   ACCOUNT       SYMBOL     TF   SETUP_TYPE")
    for n, pnl, winrate, avg_pnl, acct, sym, tf, st in rows[:50]:
        print(f"{n:<4d} {pnl:>8.2f}   {winrate:>6.1%}  {avg_pnl:>7.4f}   {acct:<12} {sym:<9} {tf:<4} {st}")

if __name__ == "__main__":
    main()
