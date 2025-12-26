from __future__ import annotations

from pathlib import Path

TARGET = Path(r"app\sim\paper_broker.py")

HEADER = """#!/usr/bin/env python3
# -*- coding: utf-8 -*-
\"\"\"
Flashback — Paper Broker (LEARN_DRY engine, v1.3)

✅ Publishes PAPER positions into the canonical positions bus:
    state/positions_bus.json

Why:
- tp_sl_manager reads positions_bus.json
- executor_v2 in PAPER mode opens positions via PaperBroker
- previously PaperBroker only wrote state/paper/<account_label>.json
  so TP/SL never saw paper positions (positions=0 forever)
\"\"\"

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Literal

import yaml  # type: ignore

try:
    from app.core.config import settings  # type: ignore
    ROOT: Path = settings.ROOT  # type: ignore
except Exception:
    ROOT = Path(__file__).resolve().parents[2]

try:
    from app.core.log import get_logger  # type: ignore
except Exception:  # pragma: no cover
    import logging
    import sys

    def get_logger(name: str) -> "logging.Logger":  # type: ignore
        logger_ = logging.getLogger(name)
        if not logger_.handlers:
            handler = logging.StreamHandler(sys.stdout)
            fmt = logging.Formatter("%(asctime)s [%(levelname)s] [%(name)s] %(message)s")
            handler.setFormatter(fmt)
            logger_.addHandler(handler)
        logger_.setLevel(logging.INFO)
        return logger_

try:
    from app.core.flashback_common import record_heartbeat  # type: ignore
except Exception:  # pragma: no cover
    def record_heartbeat(name: str) -> None:  # type: ignore[override]
        return None

try:
    from app.ai.ai_events_spine import (  # type: ignore
        build_outcome_record,
        publish_ai_event,
    )
except Exception:  # pragma: no cover
    def build_outcome_record(*args: Any, **kwargs: Any) -> Dict[str, Any]:  # type: ignore
        return {}
    def publish_ai_event(*args: Any, **kwargs: Any) -> None:  # type: ignore
        pass

# --- Outcome v1 writer (fail-soft) ---
try:
    from app.ai.outcome_writer import write_outcome_from_paper_close  # type: ignore
except Exception:
    write_outcome_from_paper_close = None  # type: ignore

log = get_logger("paper_broker")
"""

def main() -> None:
    if not TARGET.exists():
        raise SystemExit(f"FAIL: missing {TARGET}")

    raw = TARGET.read_text(encoding="utf-8", errors="ignore").replace("\t", "    ")
    lines = raw.splitlines()

    # Find the FIRST occurrence of the logger line, which should exist somewhere.
    # We'll replace everything BEFORE it with the canonical header (including log=...).
    idx = None
    for i, l in enumerate(lines):
        if l.strip() == 'log = get_logger("paper_broker")':
            idx = i
            break

    if idx is None:
        # fallback: find first def _now_ms or first dataclass and cut before it
        for i, l in enumerate(lines):
            if l.strip().startswith("def _now_ms") or l.strip().startswith("@dataclass"):
                idx = i - 1
                break

    if idx is None or idx < 0:
        raise SystemExit("FAIL: could not locate insertion point (log=get_logger or _now_ms/@dataclass).")

    # Keep everything AFTER the log line (or after idx) from the original file
    # If idx was found as log line index, keep from idx+1 onward.
    keep_from = idx + 1 if 'log = get_logger("paper_broker")' in lines[idx] else max(idx, 0)

    new_text = HEADER.rstrip("\n") + "\n" + "\n".join(lines[keep_from:]).lstrip("\n") + "\n"
    TARGET.write_text(new_text, encoding="utf-8")
    print(f"OK: rebuilt paper_broker.py header (kept_from_line={keep_from+1})")

if __name__ == "__main__":
    main()
