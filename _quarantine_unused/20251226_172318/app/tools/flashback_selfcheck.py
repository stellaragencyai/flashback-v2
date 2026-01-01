#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Startup Self-Check Utility

Purpose
-------
Run a series of sanity checks BEFORE starting the AI stack:

    • Validate project ROOT and key directories.
    • Confirm critical config files exist (strategies, subaccounts, exits).
    • Check that required env vars are present (Bybit keys, account label).
    • Smoke-test imports for core modules (config, flashback_common, ws_switchboard, tp_sl_manager, executor).

Usage
-----
From the project root:

    python -m app.tools.flashback_selfcheck

Exit code:
    0 -> all critical checks passed
    1 -> one or more critical checks failed

This is meant to be run manually AND from your supervisor/start scripts
so you don't waste time debugging something that a simple pre-flight
check could have caught.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import List, Tuple, Callable


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class CheckResult:
    def __init__(self, name: str, ok: bool, message: str, critical: bool = True) -> None:
        self.name = name
        self.ok = ok
        self.message = message
        self.critical = critical

    def icon(self) -> str:
        return "✅" if self.ok else ("❌" if self.critical else "⚠️")

    def line(self) -> str:
        return f"{self.icon()} [{self.name}] {self.message}"


def _print_header(title: str) -> None:
    bar = "─" * len(title)
    print(f"\n{title}\n{bar}")


def _safe_import(mod_name: str) -> Tuple[bool, str]:
    try:
        __import__(mod_name)
        return True, f"Imported {mod_name}"
    except Exception as e:
        return False, f"Failed to import {mod_name}: {e!r}"


# ---------------------------------------------------------------------------
# Core checks
# ---------------------------------------------------------------------------

def check_root() -> CheckResult:
    """
    Derive ROOT and verify expected structure exists.
    """
    try:
        here = Path(__file__).resolve()
        root = here.parents[2]
        expected_dirs = ["app", "config", "state"]
        missing = [d for d in expected_dirs if not (root / d).is_dir()]

        if missing:
            return CheckResult(
                "ROOT",
                ok=False,
                message=f"Derived ROOT={root} but missing dirs: {', '.join(missing)}",
                critical=True,
            )

        return CheckResult("ROOT", ok=True, message=f"ROOT appears valid at: {root}", critical=True)
    except Exception as e:
        return CheckResult("ROOT", ok=False, message=f"Error deriving ROOT: {e!r}", critical=True)


def check_config_files() -> List[CheckResult]:
    """
    Verify the existence of core config files.
    """
    here = Path(__file__).resolve()
    root = here.parents[2]
    config_dir = root / "config"

    checks: List[CheckResult] = []

    if not config_dir.is_dir():
        checks.append(
            CheckResult(
                "ConfigDir",
                ok=False,
                message=f"Config directory not found at: {config_dir}",
                critical=True,
            )
        )
        return checks

    required_files = [
        ("strategies.yaml", True),
        ("subaccounts.yaml", True),
        ("exit_profiles.yaml", False),  # some setups may name this differently, mark as warning
    ]

    for fname, critical in required_files:
        path = config_dir / fname
        if path.is_file():
            checks.append(
                CheckResult(
                    f"Config:{fname}",
                    ok=True,
                    message=f"Found config file: {path.relative_to(root)}",
                    critical=critical,
                )
            )
        else:
            checks.append(
                CheckResult(
                    f"Config:{fname}",
                    ok=False,
                    message=f"Missing config file: {path.relative_to(root)}",
                    critical=critical,
                )
            )

    return checks


def check_env_vars() -> List[CheckResult]:
    """
    Validate that required env vars exist for main account + general WS usage.

    This does NOT assume a specific naming pattern for all subaccounts,
    but it enforces at least main-level keys and a default ACCOUNT_LABEL.
    """
    checks: List[CheckResult] = []

    required_env = [
        ("BYBIT_MAIN_API_KEY", True),
        ("BYBIT_MAIN_API_SECRET", True),
    ]

    # Allow fallback to generic BYBIT_API_KEY / BYBIT_API_SECRET
    if not os.getenv("BYBIT_MAIN_API_KEY") and not os.getenv("BYBIT_API_KEY"):
        checks.append(
            CheckResult(
                "Env:BYBIT_MAIN_API_KEY",
                ok=False,
                message="Neither BYBIT_MAIN_API_KEY nor BYBIT_API_KEY is set.",
                critical=True,
            )
        )
    else:
        checks.append(
            CheckResult(
                "Env:BYBIT_MAIN_API_KEY",
                ok=True,
                message="Found Bybit main API key (MAIN or generic).",
                critical=True,
            )
        )

    if not os.getenv("BYBIT_MAIN_API_SECRET") and not os.getenv("BYBIT_API_SECRET"):
        checks.append(
            CheckResult(
                "Env:BYBIT_MAIN_API_SECRET",
                ok=False,
                message="Neither BYBIT_MAIN_API_SECRET nor BYBIT_API_SECRET is set.",
                critical=True,
            )
        )
    else:
        checks.append(
            CheckResult(
                "Env:BYBIT_MAIN_API_SECRET",
                ok=True,
                message="Found Bybit main API secret (MAIN or generic).",
                critical=True,
            )
        )

    # ACCOUNT_LABEL is important for AI stack / WS
    account_label = os.getenv("ACCOUNT_LABEL")
    if not account_label:
        checks.append(
            CheckResult(
                "Env:ACCOUNT_LABEL",
                ok=False,
                message="ACCOUNT_LABEL is not set. Default routing may break or mislabel state.",
                critical=False,
            )
        )
    else:
        checks.append(
            CheckResult(
                "Env:ACCOUNT_LABEL",
                ok=True,
                message=f"ACCOUNT_LABEL set to: {account_label}",
                critical=False,
            )
        )

    return checks


def check_core_imports() -> List[CheckResult]:
    """
    Smoke-test imports for critical core modules to catch dependency / path errors early.
    """
    modules = [
        "app.core.config",
        "app.core.flashback_common",
        "app.ws.ws_switchboard",
        "app.bots.tp_sl_manager",
        "app.core.bus_types",
    ]

    results: List[CheckResult] = []
    for mod in modules:
        ok, msg = _safe_import(mod)
        results.append(CheckResult(f"Import:{mod}", ok=ok, message=msg, critical=mod.startswith("app.core")))
    return results


def check_state_dir() -> List[CheckResult]:
    """
    Verify that core state dir exists and is writable.
    """
    here = Path(__file__).resolve()
    root = here.parents[2]
    state_dir = root / "state"

    if not state_dir.is_dir():
        return [
            CheckResult(
                "StateDir",
                ok=False,
                message=f"State directory not found at: {state_dir}",
                critical=True,
            )
        ]

    # Try to touch a temp file
    try:
        tmp = state_dir / ".selfcheck_tmp"
        tmp.write_text("ok", encoding="utf-8")
        tmp.unlink(missing_ok=True)
        return [
            CheckResult(
                "StateDir",
                ok=True,
                message=f"State directory exists and is writable: {state_dir}",
                critical=True,
            )
        ]
    except Exception as e:
        return [
            CheckResult(
                "StateDir",
                ok=False,
                message=f"State dir not writable ({state_dir}): {e!r}",
                critical=True,
            )
        ]


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

def run_selfcheck() -> bool:
    """
    Run all checks and print a compact report.
    Returns True if all CRITICAL checks passed, False otherwise.
    """
    all_results: List[CheckResult] = []

    # 1) Root / basic dirs
    _print_header("Flashback Self-Check: ROOT & Structure")
    r_root = check_root()
    print(r_root.line())
    all_results.append(r_root)

    # 2) Config files
    _print_header("Config Files")
    config_results = check_config_files()
    for r in config_results:
        print(r.line())
    all_results.extend(config_results)

    # 3) Env vars
    _print_header("Environment Variables")
    env_results = check_env_vars()
    for r in env_results:
        print(r.line())
    all_results.extend(env_results)

    # 4) Core imports
    _print_header("Core Module Imports")
    import_results = check_core_imports()
    for r in import_results:
        print(r.line())
    all_results.extend(import_results)

    # 5) State dir
    _print_header("State Directory")
    state_results = check_state_dir()
    for r in state_results:
        print(r.line())
    all_results.extend(state_results)

    # Summary
    critical_fails = [r for r in all_results if r.critical and not r.ok]

    _print_header("Summary")
    if critical_fails:
        print("❌ One or more CRITICAL checks failed.")
        for r in critical_fails:
            print(f"   - {r.name}: {r.message}")
        print("\nFix the above before running supervisor_ai_stack or TP/SL manager.")
        return False
    else:
        print("✅ All critical checks passed. Flashback core is structurally ready to launch.")
        return True


def main() -> int:
    ok = run_selfcheck()
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
