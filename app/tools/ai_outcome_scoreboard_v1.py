from __future__ import annotations

import argparse
import json
import math
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

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


def _safe_get(d: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return default


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
    account_label: Optional[str] = None


@dataclass
class BucketStats:
    key: BucketKey
    n: int
    win_rate: float
    avg_pnl: float
    median_pnl: float
    expectancy: float
    profit_factor: Optional[float]
    max_dd_proxy: Optional[float]


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


def _dd_proxy(pnls: List[float]) -> Optional[float]:
    peak = 0.0
    equity = 0.0
    max_dd = 0.0
    for p in pnls:
        equity += p
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    return max_dd


def main():
    ap = argparse.ArgumentParser("Outcome Scoreboard v1")
    ap.add_argument("--path", default=OUTCOMES_PATH_DEFAULT)
    ap.add_argument("--top", type=int, default=30)
    ap.add_argument("--min-n", type=int, default=10)
    args = ap.parse_args()

    path = Path(args.path)
    if not path.exists():
        print(f"ERROR: {path} not found")
        return

    buckets = {}

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

    stats = []
    for key, pnls in buckets.items():
        n = len(pnls)
        if n < args.min_n:
            continue

        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p < 0]

        stats.append(
            BucketStats(
                key=key,
                n=n,
                win_rate=len(wins) / n,
                avg_pnl=sum(pnls) / n,
                median_pnl=statistics.median(pnls),
                expectancy=sum(pnls) / n,
                profit_factor=(sum(wins) / abs(sum(losses))) if losses else None,
                max_dd_proxy=_dd_proxy(pnls),
            )
        )

    stats.sort(key=lambda s: s.expectancy, reverse=True)

    print("setup_type | tf | symbol | N | win% | avg_pnl | PF | maxDD")
    print("-" * 70)
    for s in stats[: args.top]:
        print(
            f"{s.key.setup_type:10} {s.key.timeframe:6} {s.key.symbol:10} "
            f"{s.n:4} {s.win_rate*100:5.1f} "
            f"{s.avg_pnl:8.4f} "
            f"{(s.profit_factor or 0):6.2f} "
            f"{(s.max_dd_proxy or 0):8.4f}"
        )


if __name__ == "__main__":
    main()
