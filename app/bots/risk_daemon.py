#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Risk Daemon (stub v0.1)

Role
----
Placeholder risk daemon so supervisor_ai_stack can manage a "risk" worker.

For now this JUST:
    - writes a heartbeat periodically

No breakers, no trade blocking yet.

Future evolution:
    - read equity / realized & unrealized PnL
    - enforce daily loss caps
    - toggle GUARD_ENABLED / GLOBAL_BREAKER / AI flags
    - write its own JSONL logs for auditing
"""

from __future__ import annotations

import time

try:
    from app.core.log import get_logger  # type: ignore
except Exception:  # pragma: no cover
    import logging, sys

    def get_logger(name: str) -> "logging.Logger":  # type: ignore
        logger_ = logging.getLogger(name)
        if not logger_.handlers:
            handler = logging.StreamHandler(sys.stdout)
            fmt = logging.Formatter(
                "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
            )
            handler.setFormatter(fmt)
            logger_.addHandler(handler)
        logger_.setLevel(logging.INFO)
        return logger_

log = get_logger("risk_daemon")

try:
    from app.core.flashback_common import record_heartbeat  # type: ignore
except Exception:  # pragma: no cover
    def record_heartbeat(name: str) -> None:  # type: ignore[override]
        return None


def main() -> None:
    log.info("Risk Daemon stub started (heartbeats only, no breakers yet).")

    while True:
        try:
            record_heartbeat("risk_daemon")
        except Exception:
            # We absolutely do not care if this fails occasionally.
            pass
        time.sleep(10)


if __name__ == "__main__":
    main()
