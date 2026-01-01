#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Execution Latency Report

Reads:
  - state/latency_exec.jsonl  (written by executor_v2)

Outputs:
  - Per-event latency stats (decision_pipeline, entry_order)
  - p50 / p90 / p99
  - Simple OK/WARN flags based on configurable thresholds
"""

from __future__ import annotations

import json
import statistics
from pathlib import Path
from typing import Any, Dict, List, Tuple

from app.core.config import settings

ROOT: Path = settings.ROOT
STATE_DIR: Path = ROOT / "state"
LATENCY_PATH: Path = STATE_DIR / "latency_exec.jsonl"


def _load_rows() -> List[Dict[str, Any]]:
    if not LATENCY_PATH.exists():
        print(f"[latency_report] No file: {LATENCY_PATH}")
        return []
    rows: List[Dict[str, Any]] = []
    with LATENCY_PATH.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                continue
    return rows


def _percentiles(data: List[float]) -> Tuple[float, float, float]:
    if not data:
        return 0.0, 0.0, 0.0
    data_sorted = sorted(data)
    n = len(data_sorted)

    def pct(p: float) -> float:
        if n == 1:
            return data_sorted[0]
        k = (n - 1) * p
        f = int(k)
        c = min(f + 1, n - 1)
        if f == c:
            return data_sorted[f]
        return data_sorted[f] + (data_sorted[c] - data_sorted[f]) * (k - f)

    return pct(0.5), pct(0.9), pct(0.99)


def main() -> None:
    rows = _load_rows()
    if not rows:
        return

    by_event: Dict[str, List[float]] = {}
    for r in rows:
        event = str(r.get("event") or "unknown")
        try:
            d = float(r.get("duration_ms") or 0)
        except Exception:
            continue
        if d <= 0:
            continue
        by_event.setdefault(event, []).append(d)

    print(f"[latency_report] Loaded {sum(len(v) for v in by_event.values())} rows "
          f"from {LATENCY_PATH}")
    print("")

    if not by_event:
        print("[latency_report] No valid latency data.")
        return

    for event, vals in sorted(by_event.items()):
        p50, p90, p99 = _percentiles(vals)
        avg = statistics.mean(vals) if vals else 0.0

        status = "OK"
        if p90 > 2000 or p99 > 3000:
            status = "BAD"
        elif p90 > 1500:
            status = "WARN"

        print(f"Event: {event}")
        print(f"  count : {len(vals)}")
        print(f"  avg   : {avg:.1f} ms")
        print(f"  p50   : {p50:.1f} ms")
        print(f"  p90   : {p90:.1f} ms")
        print(f"  p99   : {p99:.1f} ms")
        print(f"  status: {status}")
        print("")

    print("[latency_report] Done.")


if __name__ == "__main__":
    main()
