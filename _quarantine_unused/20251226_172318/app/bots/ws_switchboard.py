#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — WS Switchboard bot wrapper (ops_state-enabled)

Run:
    python -m app.bots.ws_switchboard

All logic lives in app.core.ws_switchboard.

This wrapper exists to:
- provide a canonical entrypoint for the supervisor
- write ops_snapshot status (single source of truth)
"""

from __future__ import annotations

import os
import time
import traceback
from typing import Any, Dict

from app.core.ws_switchboard import main as core_ws_main


def _now_ms() -> int:
    return int(time.time() * 1000)


def _ops_write(component: str, account_label: str, ok: bool, details: Dict[str, Any]) -> None:
    """
    Best-effort ops snapshot write.
    This must NEVER prevent the worker from running.
    """
    try:
        from app.ops.ops_state import write_component_status  # type: ignore
        write_component_status(component, account_label, ok, details)
    except Exception:
        # Fail open. WS should still run even if ops tooling is broken.
        pass


def main() -> None:
    account_label = os.getenv("ACCOUNT_LABEL", "main").strip() or "main"

    # BOOT status: the wrapper is alive and handing off to core.
    _ops_write(
        "ws_switchboard",
        account_label,
        True,
        {
            "phase": "boot",
            "ts_ms": _now_ms(),
            "note": "wrapper started; handing off to app.core.ws_switchboard.main",
        },
    )

    try:
        core_ws_main()

        # If core ever returns (usually it shouldn't), mark graceful exit.
        _ops_write(
            "ws_switchboard",
            account_label,
            True,
            {
                "phase": "exit",
                "ts_ms": _now_ms(),
                "note": "core ws_switchboard exited normally",
            },
        )
    except KeyboardInterrupt:
        _ops_write(
            "ws_switchboard",
            account_label,
            True,
            {
                "phase": "stopped",
                "ts_ms": _now_ms(),
                "note": "KeyboardInterrupt",
            },
        )
        raise
    except Exception as e:
        tb = traceback.format_exc(limit=20)
        _ops_write(
            "ws_switchboard",
            account_label,
            False,
            {
                "phase": "crash",
                "ts_ms": _now_ms(),
                "error": repr(e),
                "traceback": tb,
            },
        )
        raise


if __name__ == "__main__":
    main()
