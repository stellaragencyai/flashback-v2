#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Feature Store Builder (wrapper around app.ai.feature_builder)

Purpose
-------
Phase 3 "AI State Engine" helper.

This tool does two things:

  1) Calls the feature builder to turn:
        state/trades_log.jsonl and/or state/features_trades.jsonl
     into:
        state/feature_store.jsonl

  2) Immediately runs the AI state inspector so you can see:
        - row counts
        - top symbols / strategies
        - mode distribution (PAPER / LIVE_*)

Notes
-----
- This is **read-only** with respect to configs; it only mutates files under state/.
- If there is no trade / feature data yet, it will just produce an empty or tiny store.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

try:
    from app.core.config import settings
    ROOT: Path = settings.ROOT  # type: ignore
except Exception:
    ROOT = Path(__file__).resolve().parents[2]

STATE_DIR = ROOT / "state"
FEATURE_STORE_PATH = STATE_DIR / "feature_store.jsonl"


def _run_feature_builder() -> None:
    """
    Try to call app.ai.feature_builder in a robust way:

        - Prefer a build_feature_store() function if present.
        - Fall back to a main() / run() entrypoint if that's what's exported.

    If nothing obvious is found, emit a clear message instead of crashing
    with a useless AttributeError.
    """
    try:
        from app.ai import feature_builder  # type: ignore
    except Exception as e:
        print(f"[feature_store_builder] ERROR: cannot import app.ai.feature_builder: {e}")
        return

    # Try function-style APIs in order of preference
    for fn_name in ("build_feature_store", "build", "main", "run"):
        fn = getattr(feature_builder, fn_name, None)
        if callable(fn):
            print(f"[feature_store_builder] Using feature_builder.{fn_name}() ...")
            try:
                fn()  # type: ignore[call-arg]
            except TypeError:
                # Some older main() may expect argv, etc. We swallow that and move on.
                print(
                    f"[feature_store_builder] WARNING: feature_builder.{fn_name}() "
                    f"raised TypeError; please normalize its signature later."
                )
            return

    # If we get here, the module exists but we don't know how to drive it.
    print(
        "[feature_store_builder] ERROR: app.ai.feature_builder does not expose "
        "a build_feature_store()/build()/main()/run() function.\n"
        "Open app/ai/feature_builder.py and add a build_feature_store() function "
        "that writes state/feature_store.jsonl, then re-run this tool."
    )


def _run_inspector() -> None:
    """
    Run the AI state inspector to summarize the newly built feature store.
    """
    try:
        from app.tools import ai_state_inspector  # type: ignore
    except Exception as e:
        print(f"[feature_store_builder] WARNING: cannot import ai_state_inspector: {e}")
        return

    try:
        ai_state_inspector.main()  # type: ignore[attr-defined]
    except Exception as e:
        print(f"[feature_store_builder] WARNING: ai_state_inspector.main() failed: {e}")


def main() -> None:
    print(f"[feature_store_builder] ROOT: {ROOT}")
    print("[feature_store_builder] Building feature store via app.ai.feature_builder ...")
    _run_feature_builder()

    if FEATURE_STORE_PATH.exists():
        size_bytes = FEATURE_STORE_PATH.stat().st_size
        print(
            f"[feature_store_builder] feature_store.jsonl now exists "
            f"({size_bytes} bytes)."
        )
    else:
        print(
            "[feature_store_builder] WARNING: feature_store.jsonl does not exist "
            "after running feature_builder. This is fine if you have no trades yet."
        )

    print("[feature_store_builder] Running AI state inspector ...")
    _run_inspector()
    print("[feature_store_builder] Done.")


if __name__ == "__main__":
    main()
