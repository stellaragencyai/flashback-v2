#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import time
from pathlib import Path
import orjson

SRC = Path("state/ai_decisions.jsonl")
if not SRC.exists():
    raise SystemExit("missing state/ai_decisions.jsonl")

bak = Path(f"state/ai_decisions.jsonl.bak_compact_{int(time.time())}")
bak.write_bytes(SRC.read_bytes())

out = []
dropped = 0
kept = 0
bad = 0

for b in SRC.read_bytes().splitlines():
    s = b.strip()
    if not s or s[:1] != b"{":
        continue
    try:
        d = orjson.loads(s)
    except Exception:
        bad += 1
        continue
    if not isinstance(d, dict):
        continue

    tid = str(d.get("trade_id") or "").strip()

    # Drop obvious garbage rows from broken CLI attempts
    if tid.startswith("--") or tid == "":
        dropped += 1
        continue

    out.append(orjson.dumps(d, option=orjson.OPT_SORT_KEYS, default=str))
    kept += 1

SRC.write_bytes(b"\n".join(out) + (b"\n" if out else b""))

print("backup", bak.as_posix())
print("kept", kept)
print("dropped", dropped)
print("bad_json", bad)
