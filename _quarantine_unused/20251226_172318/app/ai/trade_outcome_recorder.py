from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict

from app.ai.outcome_writer import write_outcome_from_exec_row

log = logging.getLogger("trade_outcome_recorder")
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

STATE_DIR = Path("state")
STATE_DIR.mkdir(parents=True, exist_ok=True)

# Canonical public WS execution bus
EXEC_BUS_PATH: Path = STATE_DIR / "ws_executions.jsonl"

POLL_SEC = float(os.getenv("OUTCOME_RECORDER_POLL_SEC", "0.25"))


def _is_plausibly_close_row(row: Dict[str, Any]) -> bool:
    # Heuristic: if it contains realized PnL or explicit close markers, it’s close-ish.
    if row.get("closed_pnl") is not None:
        return True
    if row.get("realized_pnl") is not None:
        return True
    if row.get("realisedPnl") is not None:
        return True
    if row.get("close_reason") is not None:
        return True
    return False


def main() -> int:
    log.info("=== TRADE OUTCOME RECORDER (v1) ===")
    log.info("exec_bus=%s exists=%s", EXEC_BUS_PATH, EXEC_BUS_PATH.exists())

    cursor = 0

    while True:
        try:
            if not EXEC_BUS_PATH.exists():
                time.sleep(1.0)
                continue

            size = EXEC_BUS_PATH.stat().st_size
            if size < cursor:
                log.info("ws_executions.jsonl truncated (size=%s < cursor=%s), resetting cursor=0", size, cursor)
                cursor = 0

            with EXEC_BUS_PATH.open("r", encoding="utf-8", errors="ignore") as f:
                f.seek(max(0, int(cursor)))

                while True:
                    line = f.readline()
                    if not line:
                        break
                    cursor = f.tell()

                    line = line.strip()
                    if not line:
                        continue

                    try:
                        row = json.loads(line)
                    except Exception:
                        continue

                    if not isinstance(row, dict):
                        continue
                    if not _is_plausibly_close_row(row):
                        continue

                    ok, msg = write_outcome_from_exec_row(
                        exec_row=row,
                        account_label=row.get("account_label"),
                        sub_uid=row.get("sub_uid"),
                        source="trade_outcome_recorder",
                    )
                    if ok:
                        log.info("%s", msg)

        except Exception as e:
            log.exception("recorder loop error: %s", e)

        time.sleep(POLL_SEC)


if __name__ == "__main__":
    raise SystemExit(main())
