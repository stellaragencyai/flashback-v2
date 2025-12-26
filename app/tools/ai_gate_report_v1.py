from __future__ import annotations

import argparse
import json
import math
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

OUTCOMES_PATH_DEFAULT = r"state\ai_events\outcomes.v1.jsonl"


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
        return sum(1 for p in self.pnls if p > 0) / len(self.pnls)

    @property
    def expectancy(self) -> float:
        return sum(self.pnls) / len(self.pnls) if self.pnls else 0.0


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


def gate_decision(n: int, expectancy: float, win_rate: float):
    if n < 30:
        return ("INSUFFICIENT_DATA", "BLOCK", 0.0)

    if expectancy <= 0.0:
        return ("BLOCK_NEG", "BLOCK", 0.0)

    if n < 50:
        return ("HOLD_MORE_DATA", "HOLD", 0.0)

    if win_rate < 0.45:
        return ("HOLD_LOW_WINRATE", "HOLD", 0.0)

    size = 1.0
    if expectancy > 0.10:
        size = 1.5
    elif expectancy > 0.05:
        size = 1.25

    return ("ALLOW", "ALLOW", size)


def main():
    ap = argparse.ArgumentParser("Gate v1 report")
    ap.add_argument("--path", default=OUTCOMES_PATH_DEFAULT)
    ap.add_argument("--min-n", type=int, default=1)
    ap.add_argument("--top", type=int, default=50)
    args = ap.parse_args()

    path = Path(args.path)
    if not path.exists():
        print(f"ERROR: {path} not found")
        return

    buckets: Dict[BucketKey, List[float]] = {}

    for row in _iter_jsonl(path):
        if row.get("schema_version") != "outcome.v1":
            continue

        pnl = _as_float(row.get("pnl_usd"))
        if pnl is None:
            continue

        key = BucketKey(
            setup_type=str(row.get("setup_type", "unknown")),
            timeframe=str(row.get("timeframe", row.get("tf", "unknown"))),
            symbol=str(row.get("symbol", "unknown")),
        )

        buckets.setdefault(key, []).append(pnl)

    print("setup_type | tf | symbol | N | win% | expectancy | decision | action | size")
    print("-" * 95)

    for key, pnls in buckets.items():
        if len(pnls) < args.min_n:
            continue

        agg = BucketAgg(key=key, pnls=pnls)
        decision, action, size = gate_decision(agg.n, agg.expectancy, agg.win_rate)

        print(
            f"{key.setup_type:10} {key.timeframe:6} {key.symbol:10} "
            f"{agg.n:4} {agg.win_rate*100:5.1f} "
            f"{agg.expectancy:10.6f} "
            f"{decision:16} {action:6} {size:4.2f}"
        )


if __name__ == "__main__":
    main()
