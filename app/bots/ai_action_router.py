#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Action Router (Adapter / Tailer)

This module does NOT define action semantics.
It ONLY:
  - tails state/ai_actions.jsonl
  - filters by ACCOUNT_LABEL
  - calls app.core.ai_action_router.apply_ai_action(...)
  - persists offsets to avoid replay on restart
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

import orjson

from app.core.flashback_common import (
    send_tg,
    record_heartbeat,
    alert_bot_error,
)

from app.core.ai_action_router import apply_ai_action

ACCOUNT_LABEL = os.getenv("ACCOUNT_LABEL", "main").strip() or "main"
AI_ROUTER_ENABLED = os.getenv("AI_ROUTER_ENABLED", "true").strip().lower() in ("1","true","yes","on")
POLL_SECONDS = int(os.getenv("AI_ROUTER_POLL_SECONDS", "2").strip() or "2")
SEND_TG = os.getenv("AI_ROUTER_SEND_TG", "true").strip().lower() in ("1","true","yes","on")

ACTIONS_FILE = Path(os.getenv("AI_ACTIONS_PATH", "state/ai_actions.jsonl")).resolve()
OFFSET_DIR = Path("state/offsets").resolve()
OFFSET_DIR.mkdir(parents=True, exist_ok=True)
OFFSET_FILE = OFFSET_DIR / f"ai_action_router_{ACCOUNT_LABEL}.offset"


def _load_offset() -> int:
    try:
        if OFFSET_FILE.exists():
            raw = OFFSET_FILE.read_text("utf-8", errors="ignore").strip()
            return int(raw) if raw else 0
    except Exception:
        pass
    return 0


def _save_offset(off: int) -> None:
    try:
        OFFSET_FILE.write_text(str(int(off)), encoding="utf-8")
    except Exception:
        pass


def _iter_new_lines(path: Path, offset: int) -> Tuple[int, List[Dict[str, Any]]]:
    if not path.exists():
        return offset, []

    try:
        size = path.stat().st_size
    except Exception:
        return offset, []

    # truncation safety
    if offset > size:
        offset = 0

    out: List[Dict[str, Any]] = []
    try:
        with path.open("rb") as f:
            f.seek(offset)
            for line in f:
                offset += len(line)
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = orjson.loads(line)
                    if isinstance(obj, dict):
                        out.append(obj)
                except Exception:
                    continue
    except Exception as e:
        alert_bot_error("ai_action_router_adapter", f"read error: {e}", "ERROR")
        return offset, []

    return offset, out


def loop() -> None:
    if not AI_ROUTER_ENABLED:
        return

    # Load persisted offset (prevents replay)
    offset = _load_offset()

    # Startup notice
    try:
        send_tg(f"📡 AI Action Router adapter online for {ACCOUNT_LABEL}")
    except Exception:
        pass

    while True:
        record_heartbeat("ai_action_router_adapter")

        try:
            offset, envs = _iter_new_lines(ACTIONS_FILE, offset)
            if envs:
                _save_offset(offset)

            for env in envs:
                # Filter by account label (support both schema styles)
                lab = env.get("account_label") or env.get("label")
                if str(lab or "").strip() != ACCOUNT_LABEL:
                    continue

                res = apply_ai_action(env)

                # Optional TG logging (lightweight)
                if SEND_TG:
                    try:
                        ok = res.get("ok")
                        norm = res.get("normalized") or {}
                        t = norm.get("type")
                        sym = norm.get("symbol")
                        dry = norm.get("dry_run")
                        send_tg(f"🤖 AI action: ok={ok} type={t} symbol={sym} dry={dry}")
                    except Exception:
                        pass

        except Exception as e:
            alert_bot_error("ai_action_router_adapter", f"loop error: {e}", "ERROR")

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    loop()
