#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Stack Supervisor v3.3 (Hardened validator + log-safe STOP messages)

Changes (v3.3):
1) HARD GATE validator import is robust:
   - try package import (app.tools.validate_config)
   - fallback to running validate_config.py by file path (subprocess)
2) STOP messages are ASCII-only to avoid Windows cp1252 logging crashes
3) Workers: supports BOTH sync + async entry functions (fixes "coroutine was never awaited")
4) Keeps: ops_snapshot writes, per-worker telemetry, restart tracking, rate-limited alerts
"""

from __future__ import annotations

import asyncio
import inspect
import multiprocessing as mp
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

# --- PHASE8_IMPORT_PATH_SHIM ---
import os as _os
import sys as _sys
from pathlib import Path as _Path

_ROOT = _Path(__file__).resolve().parents[2]
if str(_ROOT) not in _sys.path:
    _sys.path.insert(0, str(_ROOT))

# Best-effort env defaults (NOTE: does NOT retroactively change sys.stdout encoding)
_os.environ.setdefault("PYTHONUTF8", "1")
_os.environ.setdefault("PYTHONIOENCODING", "utf-8")
# --- END PHASE8_IMPORT_PATH_SHIM ---


# ---------------------------------------------------------------------------
# Logging (import-safe)
# ---------------------------------------------------------------------------

def _get_logger():
    try:
        from app.core.log import get_logger  # type: ignore
        return get_logger("supervisor_ai_stack")
    except Exception:  # pragma: no cover
        import logging
        logger_ = logging.getLogger("supervisor_ai_stack")
        if not logger_.handlers:
            handler = logging.StreamHandler(sys.stdout)
            fmt = logging.Formatter("%(asctime)s [%(levelname)s] [%(name)s] %(message)s")
            handler.setFormatter(fmt)
            logger_.addHandler(handler)
        logger_.setLevel(logging.INFO)
        return logger_


def _ascii_safe(s: str) -> str:
    """
    Avoid Windows cp1252 stdout crashes from emoji/unicode in logging.
    """
    try:
        return s.encode("ascii", errors="replace").decode("ascii", errors="ignore")
    except Exception:
        return "MESSAGE_ENCODING_ERROR"


# ---------------------------------------------------------------------------
# ROOT + dotenv (must be called only in MainProcess)
# ---------------------------------------------------------------------------

def _resolve_root() -> Path:
    try:
        from app.core.config import settings  # type: ignore
        return Path(settings.ROOT)  # type: ignore
    except Exception:
        return Path(__file__).resolve().parents[2]


def _load_env_file(root: Path, log) -> Dict[str, str]:
    """
    Load .env into process env, and return dotenv_values (file-first behavior).
    Must run only in MainProcess to avoid spam on spawn imports.
    """
    try:  # pragma: no cover
        from dotenv import load_dotenv, dotenv_values  # type: ignore
        load_dotenv(root / ".env")
        vals = dotenv_values(root / ".env") or {}
        log.info("Loaded .env from %s", root / ".env")
        out: Dict[str, str] = {}
        for k, v in vals.items():
            if k is None or v is None:
                continue
            out[str(k)] = str(v)
        return out
    except Exception:
        log.info("Could not load .env via python-dotenv; relying on OS environment only.")
        return {}


# ---------------------------------------------------------------------------
# Ops Snapshot writer (best-effort)
# ---------------------------------------------------------------------------

def _now_ms() -> int:
    return int(time.time() * 1000)


def _ops_write(component: str, account_label: str, ok: bool, details: Dict[str, Any]) -> None:
    """
    Best-effort write into ops_snapshot.json. Never break supervisor if ops fails.
    """
    try:
        from app.ops.ops_state import write_component_status  # type: ignore
        write_component_status(
            component=component,
            account_label=account_label,
            ok=ok,
            details=details,
            ts_ms=_now_ms(),
        )
    except Exception:
        return


# ---------------------------------------------------------------------------
# Core helpers (TG / heartbeat) - import safe wrappers
# ---------------------------------------------------------------------------

def _load_common(log):
    try:
        from app.core.flashback_common import (  # type: ignore
            record_heartbeat,
            send_tg,
            alert_bot_error,
        )
        return record_heartbeat, send_tg, alert_bot_error
    except Exception:
        def record_heartbeat(name: str) -> None:
            return None

        def send_tg(msg: str) -> None:
            log.info("[TG Fallback] %s", _ascii_safe(msg))

        def alert_bot_error(bot_name: str, msg: str, level: str = "ERROR") -> None:
            safe = _ascii_safe(msg)
            if level.upper() in ("WARN", "WARNING"):
                log.warning("[%s] %s", bot_name, safe)
            else:
                log.error("[%s] %s", bot_name, safe)

        return record_heartbeat, send_tg, alert_bot_error


# ---------------------------------------------------------------------------
# Env helpers (file-first)
# ---------------------------------------------------------------------------

def _env_int(name: str, default: str) -> int:
    try:
        return int(os.getenv(name, default).strip())
    except Exception:
        return int(default)


def _file_first_bool(env_file_vars: Dict[str, str], name: str, default: str = "false") -> bool:
    if name in env_file_vars:
        raw = str(env_file_vars[name]).strip().lower()
    else:
        raw = os.getenv(name, default).strip().lower()
    return raw in ("1", "true", "yes", "y", "on")


def _file_first_bool_alias(env_file_vars: Dict[str, str], primary_name: str, alias_name: str, default: str = "false") -> bool:
    if primary_name in env_file_vars:
        raw = str(env_file_vars[primary_name]).strip().lower()
        return raw in ("1", "true", "yes", "y", "on")

    if alias_name in env_file_vars:
        raw = str(env_file_vars[alias_name]).strip().lower()
        return raw in ("1", "true", "yes", "y", "on")

    if os.getenv(primary_name) is not None:
        raw = os.getenv(primary_name, default).strip().lower()
        return raw in ("1", "true", "yes", "y", "on")

    if os.getenv(alias_name) is not None:
        raw = os.getenv(alias_name, default).strip().lower()
        return raw in ("1", "true", "yes", "y", "on")

    raw = str(default).strip().lower()
    return raw in ("1", "true", "yes", "y", "on")


# ---------------------------------------------------------------------------
# HARD GATE: Config validation (robust import + file fallback)
# ---------------------------------------------------------------------------

def _run_validator_by_path(root: Path, log) -> int:
    """
    Fallback: run validate_config.py by file path.
    Returns process return code.
    """
    candidate = root / "app" / "tools" / "validate_config.py"
    if not candidate.exists():
        log.error("STOP Config validation missing: %s", candidate)
        return 2

    try:
        p = subprocess.run([sys.executable, str(candidate)], cwd=str(root), capture_output=True, text=True)
        out = _ascii_safe(p.stdout or "")
        err = _ascii_safe(p.stderr or "")
        if out.strip():
            log.info("validate_config.py stdout:\n%s", out.strip())
        if err.strip():
            log.warning("validate_config.py stderr:\n%s", err.strip())
        return int(p.returncode or 0)
    except Exception as e:
        log.error("STOP Config validator subprocess failed: %s", _ascii_safe(repr(e)))
        return 3


def _hard_gate_validate_config(root: Path, log, send_tg) -> bool:
    try:
        from app.tools.validate_config import main as validate_config_main  # type: ignore
        rc = int(validate_config_main() or 0)
        if rc != 0:
            msg = f"STOP Config validation FAILED (rc={rc}). Refusing to start AI stack."
            log.error(_ascii_safe(msg))
            try:
                send_tg(msg)
            except Exception:
                pass
            return False

        log.info("Config validation PASS")
        return True

    except ModuleNotFoundError as e:
        msg = f"STOP Config validator import failed: {e}. Trying file-path fallback."
        log.warning(_ascii_safe(msg))
        try:
            send_tg(msg)
        except Exception:
            pass

        rc = _run_validator_by_path(root, log)
        if rc != 0:
            msg2 = f"STOP Config validation FAILED via file fallback (rc={rc}). Refusing to start AI stack."
            log.error(_ascii_safe(msg2))
            try:
                send_tg(msg2)
            except Exception:
                pass
            return False

        log.info("Config validation PASS (file fallback)")
        return True

    except Exception as e:
        msg = f"STOP Config validator crashed: {e}. Refusing to start AI stack."
        log.error(_ascii_safe(msg))
        try:
            send_tg(msg)
        except Exception:
            pass
        return False


# ---------------------------------------------------------------------------
# Subaccount gating (config/subaccounts.yaml)
# ---------------------------------------------------------------------------

def _label_ai_stack_allowed(root: Path, log, label: str) -> bool:
    sub_path = root / "config" / "subaccounts.yaml"
    if not sub_path.exists():
        return True

    try:
        import yaml  # type: ignore
    except Exception:
        log.warning("subaccounts.yaml present but PyYAML missing; default allow.")
        return True

    try:
        with sub_path.open("r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
    except Exception as e:
        log.warning("Failed to parse %s: %s. Default allow for label=%s", sub_path, e, label)
        return True

    accounts = cfg.get("accounts") or []
    if not isinstance(accounts, list):
        return True

    for acc in accounts:
        if not isinstance(acc, dict):
            continue
        acc_label = str(acc.get("account_label") or "").strip()
        if not acc_label or acc_label != label:
            continue

        enabled = acc.get("enabled", True)
        enable_ai_stack = acc.get("enable_ai_stack", False)

        if not enabled:
            log.info("subaccounts.yaml: %s enabled=false -> AI stack disabled.", label)
            return False
        if not enable_ai_stack:
            log.info("subaccounts.yaml: %s enable_ai_stack=false -> AI stack disabled.", label)
            return False

        log.info("subaccounts.yaml: %s enable_ai_stack=true -> AI stack allowed.", label)
        return True

    return True


# ---------------------------------------------------------------------------
# Dynamic import + entry helpers (supports sync + async)
# ---------------------------------------------------------------------------

def _import_first(log, mod_names: List[str]):
    last_err = None
    for m in mod_names:
        try:
            module = __import__(m, fromlist=["*"])
            log.info("Import OK: %s", m)
            return module
        except Exception as e:
            last_err = e
            log.warning("Import failed: %s (%s)", m, _ascii_safe(str(e)))
    raise ImportError(f"All imports failed: {mod_names}. Last error: {last_err}")


def _run_entry_callable(log, bot_name: str, fn: Callable[..., Any]) -> None:
    """
    Run a worker entry callable that might be:
      - normal sync function
      - async function (coroutinefunction)
      - sync function that returns a coroutine
    """
    try:
        if inspect.iscoroutinefunction(fn):
            log.info("%s entry: async %s()", bot_name, getattr(fn, "__name__", "callable"))
            asyncio.run(fn())
            return

        ret = fn()
        if inspect.iscoroutine(ret):
            log.info("%s entry: %s() returned coroutine -> asyncio.run()", bot_name, getattr(fn, "__name__", "callable"))
            asyncio.run(ret)
            return

        return
    except RuntimeError as e:
        # If an event loop is already running (rare in spawned worker),
        # fall back to creating a new loop explicitly.
        msg = _ascii_safe(str(e))
        log.warning("%s entry: runtime loop issue: %s", bot_name, msg)
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            if inspect.iscoroutinefunction(fn):
                loop.run_until_complete(fn())
            else:
                ret2 = fn()
                if inspect.iscoroutine(ret2):
                    loop.run_until_complete(ret2)
        finally:
            try:
                loop.close()
            except Exception:
                pass


def _call_entry(log, module, bot_name: str) -> None:
    for fn_name in ("main", "loop", "run"):
        fn = getattr(module, fn_name, None)
        if callable(fn):
            log.info("%s entry: %s.%s()", bot_name, module.__name__, fn_name)
            _run_entry_callable(log, bot_name, fn)
            return
    raise AttributeError(f"{module.__name__} has no callable main/loop/run")


# ---------------------------------------------------------------------------
# Worker targets (top-level for Windows)
# ---------------------------------------------------------------------------

def _run_ws_switchboard() -> None:
    log = _get_logger()
    _, _, alert_bot_error = _load_common(log)
    try:
        mod = _import_first(log, ["app.core.ws_switchboard"])
        _call_entry(log, mod, "main")
    except Exception as e:
        alert_bot_error("main", f"import/runtime error: {e}", "ERROR")


def _run_tp_sl_manager() -> None:
    log = _get_logger()
    _, _, alert_bot_error = _load_common(log)
    try:
        mod = _import_first(log, ["app.bots.tp_sl_manager"])
        _call_entry(log, mod, "tp_sl_manager")
    except Exception as e:
        alert_bot_error("tp_sl_manager", f"import/runtime error: {e}", "ERROR")


def _run_ai_pilot() -> None:
    log = _get_logger()
    _, _, alert_bot_error = _load_common(log)
    try:
        mod = _import_first(log, ["app.bots.ai_pilot"])
        _call_entry(log, mod, "ai_pilot")
    except Exception as e:
        alert_bot_error("ai_pilot", f"import/runtime error: {e}", "ERROR")


def _run_ai_action_router() -> None:
    log = _get_logger()
    _, _, alert_bot_error = _load_common(log)
    try:
        mod = _import_first(log, ["app.bots.ai_action_router"])
        _call_entry(log, mod, "ai_action_router")
    except Exception as e:
        alert_bot_error("ai_action_router", f"import/runtime error: {e}", "ERROR")


def _run_ai_journal() -> None:
    log = _get_logger()
    record_heartbeat, _, _ = _load_common(log)
    log.info("ai_journal is a no-op; disable with AI_STACK_ENABLE_AI_JOURNAL=false.")
    while True:
        record_heartbeat("ai_journal")
        time.sleep(60)


def _run_risk_daemon() -> None:
    log = _get_logger()
    _, _, alert_bot_error = _load_common(log)
    try:
        mod = _import_first(log, ["app.bots.risk_daemon"])
        _call_entry(log, mod, "risk_daemon")
    except Exception as e:
        alert_bot_error("risk_daemon", f"import/runtime error (optional): {e}", "WARN")


def _run_trade_outcomes() -> None:
    log = _get_logger()
    _, _, alert_bot_error = _load_common(log)
    try:
        mod = _import_first(log, ["app.ai.trade_outcome_recorder", "app.bots.trade_outcome_recorder"])
        _call_entry(log, mod, "trade_outcomes")
    except Exception as e:
        alert_bot_error("trade_outcomes", f"import/runtime error (optional): {e}", "WARN")


def _run_paper_price_feeder() -> None:
    log = _get_logger()
    _, _, alert_bot_error = _load_common(log)
    try:
        mod = _import_first(log, ["app.sim.paper_price_feeder"])
        _call_entry(log, mod, "paper_price_feeder")
    except Exception as e:
        alert_bot_error("paper_price_feeder", f"import/runtime error (optional): {e}", "WARN")


# ---------------------------------------------------------------------------
# Worker registry
# ---------------------------------------------------------------------------

class WorkerSpec:
    def __init__(self, name: str, enabled: bool, target: Callable[[], None]) -> None:
        self.name = name
        self.enabled = enabled
        self.target = target
        self.process: Optional[mp.Process] = None
        self.restart_count: int = 0
        self.last_restart_ms: int = 0
        self.last_exitcode: Optional[int] = None
        self.last_reason: str = ""


def _build_worker_specs(env_file_vars: Dict[str, str]) -> Dict[str, WorkerSpec]:
    ws = _file_first_bool(env_file_vars, "AI_STACK_ENABLE_WS_SWITCHBOARD", "true")
    tp = _file_first_bool(env_file_vars, "AI_STACK_ENABLE_TP_SL_MANAGER", "true")
    pilot = _file_first_bool(env_file_vars, "AI_STACK_ENABLE_AI_PILOT", "true")
    router = _file_first_bool(env_file_vars, "AI_STACK_ENABLE_AI_ACTION_ROUTER", "true")
    journal = _file_first_bool(env_file_vars, "AI_STACK_ENABLE_AI_JOURNAL", "false")
    risk = _file_first_bool(env_file_vars, "AI_STACK_ENABLE_RISK_DAEMON", "false")
    outcomes = _file_first_bool(env_file_vars, "AI_STACK_ENABLE_TRADE_OUTCOMES", "true")
    paper = _file_first_bool_alias(env_file_vars, "AI_STACK_ENABLE_PAPER_PRICE_FEEDER", "AI_STACK_ENABLE_PAPER_TICK_DAEMON", "true")

    return {
        "ws_switchboard": WorkerSpec("ws_switchboard", ws, _run_ws_switchboard),
        "tp_sl_manager": WorkerSpec("tp_sl_manager", tp, _run_tp_sl_manager),
        "ai_pilot": WorkerSpec("ai_pilot", pilot, _run_ai_pilot),
        "ai_action_router": WorkerSpec("ai_action_router", router, _run_ai_action_router),
        "ai_journal": WorkerSpec("ai_journal", journal, _run_ai_journal),
        "risk_daemon": WorkerSpec("risk_daemon", risk, _run_risk_daemon),
        "trade_outcomes": WorkerSpec("trade_outcomes", outcomes, _run_trade_outcomes),
        "paper_price_feeder": WorkerSpec("paper_price_feeder", paper, _run_paper_price_feeder),
    }


# ---------------------------------------------------------------------------
# Alert rate limiting
# ---------------------------------------------------------------------------

def _should_alert(last_alert_ms: int, min_interval_sec: int) -> bool:
    if min_interval_sec <= 0:
        return True
    return (_now_ms() - last_alert_ms) >= int(min_interval_sec * 1000)


# ---------------------------------------------------------------------------
# Supervisor core
# ---------------------------------------------------------------------------

def _start_worker(log, spec: WorkerSpec) -> None:
    if spec.process is not None and spec.process.is_alive():
        return
    log.info("Starting worker %s ...", spec.name)
    p = mp.Process(target=spec.target, name=f"fb_{spec.name}", daemon=False)
    p.start()
    spec.process = p
    log.info("Worker %s started with pid=%s", spec.name, p.pid)


def _stop_worker(log, spec: WorkerSpec) -> None:
    p = spec.process
    if p is None:
        return

    if not p.is_alive():
        spec.process = None
        return

    log.info("Stopping worker %s (pid=%s) ...", spec.name, p.pid)
    try:
        p.terminate()
    except Exception:
        pass

    try:
        p.join(timeout=10)
    except Exception:
        pass

    if p.is_alive():
        try:
            os.kill(p.pid, signal.SIGKILL)  # type: ignore[arg-type]
        except Exception:
            pass

    spec.process = None
    log.info("Worker %s stopped.", spec.name)


def _supervisor_loop(root: Path, account_label: str, poll_seconds: int, env_file_vars: Dict[str, str]) -> None:
    log = _get_logger()
    record_heartbeat, send_tg, alert_bot_error = _load_common(log)

    alert_min_sec = int(os.getenv("AI_STACK_ALERT_MIN_INTERVAL_SEC", "20") or "20")
    last_alert_ms = 0

    log.info("BOOT | ROOT=%s | ACCOUNT_LABEL=%s | poll=%ss", root, account_label, poll_seconds)

    kill_switch_path = Path(_ROOT) / "state" / "KILL_SWITCH"
    if kill_switch_path.exists():
        msg = f"STOP KILL SWITCH ACTIVE: {kill_switch_path}"
        log.error(_ascii_safe(msg))
        _ops_write("supervisor_ai_stack", account_label, False, {"phase": "blocked", "reason": "kill_switch"})
        raise SystemExit(msg)

    specs = _build_worker_specs(env_file_vars)

    log.info(
        "Flags (file-first): WS=%s TP/SL=%s PILOT=%s ROUTER=%s RISK=%s OUTCOMES=%s PAPER_FEED=%s",
        specs["ws_switchboard"].enabled,
        specs["tp_sl_manager"].enabled,
        specs["ai_pilot"].enabled,
        specs["ai_action_router"].enabled,
        specs["risk_daemon"].enabled,
        specs["trade_outcomes"].enabled,
        specs["paper_price_feeder"].enabled,
    )

    log.info("AI Stack Supervisor starting for ACCOUNT_LABEL=%s (poll=%ss)", account_label, poll_seconds)

    _ops_write(
        component="supervisor_ai_stack",
        account_label=account_label,
        ok=True,
        details={"phase": "boot", "poll_seconds": poll_seconds, "note": "supervisor online"},
    )

    try:
        send_tg(f"AI Stack Supervisor online (label={account_label}, poll={poll_seconds}s)")
    except Exception:
        pass

    for name, spec in specs.items():
        log.info("Worker %-18s enabled=%s", name, spec.enabled)

    while True:
        record_heartbeat("supervisor_ai_stack")
        record_heartbeat(f"supervisor_ai_stack:{account_label}")

        enabled_names: List[str] = []
        running_names: List[str] = []
        dead_names: List[str] = []

        if kill_switch_path.exists():
            msg = f"STOP KILL SWITCH ACTIVE: {kill_switch_path}"
            log.error(_ascii_safe(msg))
            _ops_write("supervisor_ai_stack", account_label, False, {"phase": "blocked", "reason": "kill_switch"})
            raise SystemExit(msg)

        for name, spec in specs.items():
            if spec.enabled:
                enabled_names.append(name)

            if not spec.enabled:
                if spec.process is not None and spec.process.is_alive():
                    log.info("Worker %s disabled -> stopping.", name)
                    _stop_worker(log, spec)
                _ops_write(f"worker_{name}", account_label, True, {"enabled": False, "state": "disabled"})
                continue

            alive = (spec.process is not None and spec.process.is_alive())
            if not alive:
                if spec.process is not None:
                    spec.last_exitcode = spec.process.exitcode
                    spec.restart_count += 1
                    spec.last_restart_ms = _now_ms()
                    spec.last_reason = f"died exitcode={spec.last_exitcode}"

                    if _should_alert(last_alert_ms, alert_min_sec):
                        last_alert_ms = _now_ms()
                        alert_bot_error(
                            "supervisor_ai_stack",
                            f"Worker {name} died (label={account_label}, exitcode={spec.last_exitcode}); restarting.",
                            "WARN",
                        )
                else:
                    spec.last_reason = "not_started"

                _start_worker(log, spec)

            alive = (spec.process is not None and spec.process.is_alive())
            pid = spec.process.pid if spec.process is not None else None
            if alive:
                running_names.append(name)
            else:
                dead_names.append(name)

            _ops_write(
                f"worker_{name}",
                account_label,
                bool(alive),
                {
                    "enabled": True,
                    "alive": bool(alive),
                    "pid": pid,
                    "last_exitcode": spec.last_exitcode,
                    "restart_count": spec.restart_count,
                    "last_restart_ms": spec.last_restart_ms,
                    "last_reason": spec.last_reason,
                },
            )

        ok_stack = (len(dead_names) == 0)
        _ops_write(
            "supervisor_ai_stack",
            account_label,
            ok_stack,
            {
                "phase": "running",
                "poll_seconds": poll_seconds,
                "enabled": enabled_names,
                "running": running_names,
                "dead": dead_names,
                "counts": {"enabled": len(enabled_names), "running": len(running_names), "dead": len(dead_names)},
            },
        )

        time.sleep(poll_seconds)


def main() -> None:
    try:
        mp.set_start_method("spawn", force=False)
    except RuntimeError:
        pass

    log = _get_logger()
    root = _resolve_root()

    if mp.current_process().name != "MainProcess":
        return

    env_file_vars = _load_env_file(root, log)

    account_label = os.getenv("ACCOUNT_LABEL", "main").strip() or "main"
    poll_seconds = _env_int("AI_STACK_SUPERVISOR_POLL_SECONDS", "3")

    record_heartbeat, send_tg, _ = _load_common(log)

    if not _label_ai_stack_allowed(root, log, account_label):
        msg = f"AI Stack Supervisor disabled for label={account_label} by config/subaccounts.yaml"
        log.info(_ascii_safe(msg))
        _ops_write("supervisor_ai_stack", account_label, False, {"phase": "disabled", "reason": "subaccounts.yaml gate"})
        try:
            send_tg(f"STOP {msg}")
        except Exception:
            pass
        return

    if not _hard_gate_validate_config(root, log, send_tg):
        _ops_write("supervisor_ai_stack", account_label, False, {"phase": "blocked", "reason": "config validation failed"})
        return

    try:
        _supervisor_loop(root, account_label, poll_seconds, env_file_vars)
    except KeyboardInterrupt:
        log.info("AI Stack Supervisor interrupted by user; shutting down...")
        record_heartbeat("supervisor_ai_stack_stopped")
        _ops_write("supervisor_ai_stack", account_label, False, {"phase": "stopped", "reason": "KeyboardInterrupt"})
    except Exception as e:
        log.exception("supervisor_ai_stack fatal error: %s", _ascii_safe(str(e)))
        _ops_write("supervisor_ai_stack", account_label, False, {"phase": "fatal", "error": _ascii_safe(str(e))})
        raise


if __name__ == "__main__":
    try:
        mp.freeze_support()
    except Exception:
        pass
    main()
