#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI State Inspector

Purpose
-------
Quickly inspect the AI feature logs:

    - state/features_trades.jsonl  (per-trade feature snapshots at entry)
    - state/feature_store.jsonl    (enriched OHLCV/TA feature store)

and print:

    - Row counts
    - Unique symbols / strategies
    - Mode distribution (PAPER / LIVE_* / UNKNOWN)
    - Simple per-symbol counts (top N)

This is **read-only** and meant for sanity + "how much data do I have?"
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

try:
    import orjson
except Exception:
    orjson = None  # type: ignore

# Try to respect central ROOT config
try:
    from app.core.config import settings
    ROOT: Path = settings.ROOT  # type: ignore
except Exception:
    ROOT = Path(__file__).resolve().parents[2]

STATE_DIR = ROOT / "state"
FEATURES_TRADES_PATH = STATE_DIR / "features_trades.jsonl"
FEATURE_STORE_PATH = STATE_DIR / "feature_store.jsonl"


def _load_jsonl(path: Path, max_rows: Optional[int] = None) -> Iterable[Dict[str, Any]]:
    if not path.exists():
        print(f"[ai_state_inspector] File not found: {path}")
        return []

    loader = None
    if orjson is not None:
        def loader(b: bytes) -> Any:
            return orjson.loads(b)
    else:
        import json

        def loader(b: bytes) -> Any:  # type: ignore
            if isinstance(b, (bytes, bytearray)):
                b = b.decode("utf-8")
            return json.loads(b)

    rows: list[Dict[str, Any]] = []
    with path.open("rb") as f:
        for line in f:
            if max_rows is not None and len(rows) >= max_rows:
                break
            line = line.strip()
            if not line:
                continue
            try:
                obj = loader(line)
            except Exception:
                continue
            if isinstance(obj, dict):
                rows.append(obj)
    return rows


def _inspect_features_trades() -> None:
    print("\n[ai_state_inspector] Inspecting features_trades.jsonl ...")

    rows = list(_load_jsonl(FEATURES_TRADES_PATH, max_rows=100000))

    total = len(rows)
    print(f"[ai_state_inspector] features_trades rows: {total}")

    if total == 0:
        return

    symbols = Counter()
    strategies = Counter()
    modes = Counter()

    for r in rows:
        sym = str(r.get("symbol") or "").upper()
        strategies.update([str(r.get("strategy_name") or "unknown")])
        symbols.update([sym or "UNKNOWN"])

        mode = str(r.get("mode") or "").upper()
        if mode not in ("PAPER", "LIVE_CANARY", "LIVE_FULL"):
            mode = "UNKNOWN"
        modes.update([mode])

    print("[ai_state_inspector] Top symbols (features_trades):")
    for sym, cnt in symbols.most_common(10):
        print(f"  - {sym:10s}: {cnt:6d}")

    print("[ai_state_inspector] Top strategies (features_trades):")
    for name, cnt in strategies.most_common(10):
        print(f"  - {name:20s}: {cnt:6d}")

    print("[ai_state_inspector] Mode distribution (features_trades):")
    total_modes = sum(modes.values()) or 1
    for m, cnt in modes.items():
        pct = 100.0 * cnt / total_modes
        print(f"  - {m:12s}: {cnt:6d} ({pct:5.1f}%)")


def _inspect_feature_store() -> None:
    print("\n[ai_state_inspector] Inspecting feature_store.jsonl ...")
    rows = list(_load_jsonl(FEATURE_STORE_PATH, max_rows=100000))

    total = len(rows)
    print(f"[ai_state_inspector] feature_store rows: {total}")

    if total == 0:
        return

    symbols = Counter()
    strategies = Counter()
    modes = Counter()

    for r in rows:
        sym = str(r.get("symbol") or "").upper()
        symbols.update([sym or "UNKNOWN"])

        strategies.update([str(r.get("strategy_name") or "unknown")])

        mode = str(r.get("mode") or "").upper()
        if mode not in ("PAPER", "LIVE_CANARY", "LIVE_FULL"):
            mode = "UNKNOWN"
        modes.update([mode])

    print("[ai_state_inspector] Top symbols (feature_store):")
    for sym, cnt in symbols.most_common(10):
        print(f"  - {sym:10s}: {cnt:6d}")

    print("[ai_state_inspector] Top strategies (feature_store):")
    for name, cnt in strategies.most_common(10):
        print(f"  - {name:20s}: {cnt:6d}")

    print("[ai_state_inspector] Mode distribution (feature_store):")
    total_modes = sum(modes.values()) or 1
    for m, cnt in modes.items():
        pct = 100.0 * cnt / total_modes
        print(f"  - {m:12s}: {cnt:6d} ({pct:5.1f}%)")


def main() -> None:
    print(f"[ai_state_inspector] ROOT: {ROOT}")
    _inspect_features_trades()
    _inspect_feature_store()
    print("\n[ai_state_inspector] Done.")


if __name__ == "__main__":
    main()
