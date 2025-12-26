from pathlib import Path

CONTENT = r"""
from __future__ import annotations

import argparse
import json
import math
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

OUTCOMES_PATH_DEFAULT = r"state\\ai_events\\outcomes.v1.jsonl"


def _try_import_orjson():
    try:
        import orjson  # type: ignore
        return orjson
    except Exception:
        return None


_ORJSON = _try_import_orjson()


def _loads(line: str) -> Dict[str, Any]:
    if _ORJSON is not None:
        return _ORJSON.loads(line)
    return json.loads(line)


def _as_float(x: Any) -> Optional[float]:
    try:
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except Exception:
        return None


@dataclass(frozen=True)
class BucketKey:
    setup_type: str
    timeframe: str
    symbol: str

    def tup(self) -> Tuple[str, str, str]:
        return (self.setup_type, self.timeframe, self.symbol)


@dataclass
class BucketAgg:
    key: BucketKey
    pnls: List[float]

    @property
    def n(self) -> int:
        return len(self.pnls)

    @property
    def win_rate(self) -> float:
        if not self.pnls:
            return 0.0
        wins = sum(1 for p in self.pnls if p > 0)
        return wins / len(self.pnls)

    @property
    def avg_pnl(self) -> float:
        if not self.pnls:
            return 0.0
        return sum(self.pnls) / len(self.pnls)

    @property
    def median_pnl(self) -> float:
        if not self.pnls:
            return 0.0
        return float(statistics.median(self.pnls))

    @property
    def expectancy(self) -> float:
        # Scoreboard v1 proxy: avg pnl per trade in USD
        return self.avg_pnl


def _iter_jsonl(path: Path):
    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield _loads(line)
            except Exception:
                continue


def gate_decision(n: int, expectancy: float, win_rate: float) -> Tuple[str, str, float]:
    # Returns: (decision_code, action, size_multiplier)
    if n < 30:
        return ("INSUFFICIENT_DATA", "BLOCK", 0.0)

    if expectancy <= 0.0:
        return ("BLOCK_NEG", "BLOCK", 0.0)

    if n < 50:
        return ("HOLD_MORE_DATA", "HOLD", 0.0)

    # n >= 50 and positive expectancy
    if win_rate < 0.45:
        return ("HOLD_LOW_WINRATE", "HOLD", 0.0)

    size = 1.00
    if expectancy > 0.10:
        size = 1.50
    elif expectancy > 0.05:
        size = 1.25

    return ("ALLOW", "ALLOW", min(size, 1.50))


def main() -> int:
    ap = argparse.ArgumentParser(description="Gate v1 report (block/allow/size) from outcomes.v1.")
    ap.add_argument("--path", default=OUTCOMES_PATH_DEFAULT, help="Path to outcomes.v1.jsonl")
    ap.add_argument("--top", type=int, default=50, help="How many rows to show")
    ap.add_argument("--min-n", type=int, default=1, help="Minimum N to include in report (display filter)")
    ap.add_argument("--sort-by", default="expectancy", choices=["expectancy", "n", "win_rate"], help="Sort key")
    args = ap.parse_args()

    outcomes_path = Path(args.path)
    if not outcomes_path.exists():
        print(f"ERROR: outcomes path not found: {outcomes_path}")
        return 2

    buckets: Dict[BucketKey, List[float]] = {}

    for row in _iter_jsonl(outcomes_path):
        if row.get("schema_version") != "outcome.v1":
            continue

        symbol = str(row.get("symbol", "")).strip()
        if not symbol:
            continue

        setup_type = str(row.get("setup_type", "unknown")).strip() or "unknown"
        timeframe = str(row.get("timeframe", row.get("tf", "unknown"))).strip() or "unknown"

        pnl = _as_float(row.get("pnl_usd"))
        if pnl is None:
            continue

        key = BucketKey(setup_type=setup_type, timeframe=timeframe, symbol=symbol)
        buckets.setdefault(key, []).append(float(pnl))

    aggs: List[BucketAgg] = [BucketAgg(key=k, pnls=v) for k, v in buckets.items() if len(v) >= int(args.min_n)]

    if not aggs:
        print("No buckets found (after filters/min-n).")
        return 0

    if args.sort_by == "n":
        aggs.sort(key=lambda a: (a.n, a.expectancy, a.win_rate, a.key.tup()), reverse=True)
    elif args.sort_by == "win_rate":
        aggs.sort(key=lambda a: (a.win_rate, a.n, a.expectancy, a.key.tup()), reverse=True)
    else:
        aggs.sort(key=lambda a: (a.expectancy, a.n, a.win_rate, a.key.tup()), reverse=True)

    aggs = aggs[: int(args.top)] if int(args.top) > 0 else aggs

    print("setup_type | tf | symbol | N | win% | exp(avg_pnl) | decision | action | size_mult")
    print("-" * 100)

    for a in aggs:
        decision, action, size = gate_decision(a.n, a.expectancy, a.win_rate)
        print(
            f"{a.key.setup_type:10} {a.key.timeframe:6} {a.key.symbol:10} "
            f"{a.n:4} {a.win_rate*100:5.1f} {a.expectancy:12.6f} "
            f"{decision:16} {action:6} {size:8.2f}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
"""

out = Path("app/tools/ai_gate_report_v1.py")
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(CONTENT.strip() + "\\n", encoding="utf-8")
print(f"OK: created {out}")
"""

p = Path(r"app\tools\_patch_create_ai_gate_report_v1.py")
p.write_text(CONTENT.strip() + "\n", encoding="utf-8")
print("OK: wrote patch (self-overwrite guard)")
