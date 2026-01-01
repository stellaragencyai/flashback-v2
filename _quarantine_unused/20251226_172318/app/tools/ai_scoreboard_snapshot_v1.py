from __future__ import annotations

import argparse
import json
import math
import statistics
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

IN_DEFAULT = r"state\ai_events\outcomes.v1.jsonl"
OUT_DEFAULT = r"state\ai_memory\scoreboard.v1.json"


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


def _iter_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield _loads(line)
            except Exception:
                continue


def _dd_proxy(pnls: List[float]) -> float:
    peak = 0.0
    equity = 0.0
    max_dd = 0.0
    for p in pnls:
        equity += p
        if equity > peak:
            peak = equity
        dd = peak - equity
        if dd > max_dd:
            max_dd = dd
    return float(max_dd)


def _profit_factor(wins: List[float], losses: List[float]) -> Optional[float]:
    if not losses:
        return None
    s_w = sum(wins)
    s_l = abs(sum(losses))
    if s_l <= 0:
        return None
    pf = s_w / s_l
    if math.isnan(pf) or math.isinf(pf):
        return None
    return float(pf)


def _confidence(n: int, min_n: int) -> float:
    # Simple bounded confidence: ramps from 0.0 at min_n to ~1.0 as n grows.
    # This is not “AI”. It is “don’t overfit like a clown”.
    if n <= 0:
        return 0.0
    base = n / max(min_n, 1)
    c = 1.0 - math.exp(-base)
    if c < 0:
        c = 0.0
    if c > 1:
        c = 1.0
    return float(c)


def _recommended_action(expectancy: float, pf: Optional[float], win_rate: float, conf: float) -> str:
    # Conservative default: block unless we have decent confidence AND positive expectancy.
    if conf < 0.60:
        return "COLD_START"
    if expectancy <= 0:
        return "BLOCK"
    # A little extra sanity: if PF exists and is awful, block.
    if pf is not None and pf < 1.1:
        return "BLOCK"
    # Otherwise allow.
    return "ALLOW"


def main() -> int:
    ap = argparse.ArgumentParser("Scoreboard Snapshot v1")
    ap.add_argument("--in", dest="in_path", default=IN_DEFAULT)
    ap.add_argument("--out", dest="out_path", default=OUT_DEFAULT)
    ap.add_argument("--min-n", type=int, default=10)
    ap.add_argument("--min-conf", type=float, default=None)
    ap.add_argument("--max-buckets", type=int, default=5000)
    args = ap.parse_args()

    in_path = Path(args.in_path)
    out_path = Path(args.out_path)
    min_n = int(args.min_n)

    if not in_path.exists():
        print(f"ERROR: input not found: {in_path}")
        return 2

    buckets: Dict[Tuple[str, str, str], List[float]] = {}

    for row in _iter_jsonl(in_path):
        if row.get("schema_version") != "outcome.v1":
            continue

        pnl = _as_float(_safe_get(row, "pnl_usd"))
        if pnl is None:
            continue

        setup_type = str(_safe_get(row, "setup_type", default="unknown")).strip() or "unknown"
        symbol = str(_safe_get(row, "symbol", default="unknown")).strip() or "unknown"
        timeframe = str(_safe_get(row, "timeframe", "tf", default="unknown")).strip() or "unknown"

        # Hygiene: drop obviously fake / dev-only keys
        if setup_type in ("this_is_not_real", "tick", "emit_test_signal"):
            continue

        key = (setup_type, symbol, timeframe)
        buckets.setdefault(key, []).append(float(pnl))

    # Enforce max buckets to avoid runaway files
    if len(buckets) > args.max_buckets:
        print(f"ERROR: too many buckets ({len(buckets)}) > max ({args.max_buckets}). Refusing to write.")
        return 3

    out_buckets: List[Dict[str, Any]] = []

    # Deterministic ordering: sort keys
    for (setup_type, symbol, timeframe) in sorted(buckets.keys()):
        pnls = buckets[(setup_type, symbol, timeframe)]
        n = len(pnls)
        if n < min_n:
            continue  # ✅ REAL min-n enforcement

        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p < 0]

        expectancy = sum(pnls) / n
        win_rate = len(wins) / n
        pf = _profit_factor(wins, losses)
        dd = _dd_proxy(pnls)
        conf = _confidence(n, min_n)

        rec = _recommended_action(expectancy=expectancy, pf=pf, win_rate=win_rate, conf=conf)

        out_buckets.append(
            {
                "bucket_key": {"setup_type": setup_type, "symbol": symbol, "timeframe": timeframe},
                "n": n,
                "win_rate": float(win_rate),
                "expectancy": float(expectancy),
                "avg_pnl": float(expectancy),
                "median_pnl": float(statistics.median(pnls)),
                "profit_factor": pf,
                "max_dd_proxy": float(dd),
                "confidence": float(conf),
                "recommended_action": rec,
            }
        )

    payload: Dict[str, Any] = {
        "schema_version": "scoreboard.v1",
        "generated_at": int(time.time()),
        "min_n": min_n,
                "min_conf": args.min_conf,
"source": str(in_path).replace("\\", "/"),
        "buckets": out_buckets,
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Write stable JSON (no Infinity/NaN)
    txt = json.dumps(payload, indent=2, sort_keys=False)
    if "Infinity" in txt or "NaN" in txt:
        print("ERROR: output contains Infinity/NaN, refusing to write.")
        return 4

    out_path.write_text(txt, encoding="utf-8", newline="\n")
    print(f"OK: wrote {len(out_buckets)} buckets to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
