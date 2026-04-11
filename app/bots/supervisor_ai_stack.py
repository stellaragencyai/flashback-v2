"""
Flashback — AI Stack Supervisor v3.9.3
(HARD runtime pinning + spawn executable hard lock + validator hard runtime + log-safe STOP + ORCH_ENV_LOADED support)

Core guarantees:
- If supervisor is not running under the expected repo runtime python -> HARD STOP (AT IMPORT TIME).
- Multiprocessing spawn executable is pinned to the expected repo runtime python -> HARD PIN (AT IMPORT TIME).
- If mp/spawn get_executable != expected runtime python -> HARD STOP (prevents drift).
- Validator subprocess runs under expected runtime python.

Keeps:
- robust validator (import + file fallback)
- ASCII-safe logging
- sync + async worker entry support
- ops_snapshot writes, per-worker telemetry, restart tracking, rate-limited alerts

v3.9.3 change:
- trade_outcome_recorder import order fixed: prefers app.bots.trade_outcome_recorder (REAL) over app.ai.trade_outcome_recorder (STUB)
- refuses stub outcomes unless ALLOW_OUTCOME_STUB=1
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

# ---------------------------------------------------------------------------
# ABSOLUTE EARLY BOOTSTRAP (IMPORT-TIME HARD GATE)
# ---------------------------------------------------------------------------

def _bootstrap_root() -> Path:
    # supervisor_ai_stack.py lives at: <ROOT>\app\bots\supervisor_ai_stack.py
    return Path(__file__).resolve().parents[2]


def _truthy_env(name: str, default: str = "0") -> bool:
    return str(os.getenv(name, default)).strip().lower() in ("1", "true", "yes", "y", "on")


def _bootstrap_expected_venv_python(root: Path) -> Path:
    if os.name == "nt":
        return (root / ".venv" / "Scripts" / "python.exe").resolve()
    return (root / ".venv" / "bin" / "python")


def _bootstrap_expected_runtime_python(root: Path) -> Path:
    if os.name == "nt" and _truthy_env("FLASHBACK_DIRECT_BASE_PYTHON", "0"):
        override = str(os.getenv("FLASHBACK_RUNTIME_BASE_PYTHON", "")).strip()
        if override:
            candidate = Path(override)
            if candidate.exists():
                return candidate.resolve()
        base_exe = getattr(sys, "_base_executable", "") or sys.executable
        candidate = Path(str(base_exe))
        if candidate.exists():
            return candidate.resolve()
    return _bootstrap_expected_venv_python(root)


def _expected_venv_dir(root: Path) -> Path:
    return (root / ".venv").resolve()


def _expected_site_packages(root: Path) -> Path:
    if os.name == "nt":
        return (root / ".venv" / "Lib" / "site-packages").resolve()
    return (root / ".venv" / "lib" / f"python{sys.version_info.major}.{sys.version_info.minor}" / "site-packages").resolve()


def _current_python_executable() -> Path:
    current = Path(sys.executable)
    if os.name == "nt":
        return current.resolve()
    return current


def _direct_base_runtime_active(root: Path) -> bool:
    if os.name != "nt":
        return False
    if not _truthy_env("FLASHBACK_DIRECT_BASE_PYTHON", "0"):
        return False
    venv_dir = _expected_venv_dir(root)
    runtime_venv = str(os.getenv("FLASHBACK_RUNTIME_VENV", "")).strip()
    if runtime_venv:
        try:
            return Path(runtime_venv).resolve() == venv_dir
        except Exception:
            return False
    return True


def _pythonpath_contains(path_value: str, target: Path) -> bool:
    target_norm = str(target.resolve()).lower()
    for part in str(path_value or "").split(os.pathsep):
        chunk = part.strip()
        if not chunk:
            continue
        try:
            if str(Path(chunk).resolve()).lower() == target_norm:
                return True
        except Exception:
            if chunk.lower() == target_norm:
                return True
    return False


def _running_in_expected_runtime(root: Path, expected_py: Path) -> bool:
    if os.name == "nt":
        if _current_python_executable() != expected_py:
            return False
        if _direct_base_runtime_active(root):
            site_packages = _expected_site_packages(root)
            venv_dir = _expected_venv_dir(root)
            return (
                _pythonpath_contains(os.getenv("PYTHONPATH", ""), root)
                and _pythonpath_contains(os.getenv("PYTHONPATH", ""), site_packages)
                and str(os.getenv("VIRTUAL_ENV", "")).strip() == str(venv_dir)
            )
        return True

    expected_dir = _expected_venv_dir(root)

    try:
        if Path(sys.prefix).resolve() == expected_dir:
            return True
    except Exception:
        pass

    venv_env = os.getenv("VIRTUAL_ENV", "").strip()
    if venv_env:
        try:
            if Path(venv_env).resolve() == expected_dir:
                return True
        except Exception:
            pass

    return _current_python_executable() == expected_py


def _bootstrap_hard_gate_and_pin() -> None:
    """
    MUST run at import time.
    If this module is executed by system Python (e.g., Python312), kill it immediately.
    Also pins multiprocessing executable early so Windows spawn can't drift.
    """
    root = _bootstrap_root()
    expected = _bootstrap_expected_runtime_python(root)
    actual = _current_python_executable()

    # Make environment deterministic and hostile to user-site pollution
    os.environ.setdefault("PYTHONNOUSERSITE", "1")
    os.environ.setdefault("PYTHONUTF8", "1")
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")

    if not expected.exists():
        # If the venv doesn't exist, nothing should proceed.
        sys.stderr.write(f"STOP Missing expected runtime python: {expected}\n")
        raise SystemExit(78)

    if not _running_in_expected_runtime(root, expected):
        # This is the BIG fix: kill Python312 invocations instantly.
        sys.stderr.write(
            "STOP Interpreter HARD GATE FAIL (IMPORT-TIME):\n"
            f"  sys.executable = {actual}\n"
            f"  expected       = {expected}\n"
            f"  sys.prefix     = {sys.prefix}\n"
            f"  VIRTUAL_ENV    = {os.getenv('VIRTUAL_ENV', '')}\n"
            f"  direct_base    = {os.getenv('FLASHBACK_DIRECT_BASE_PYTHON', '')}\n"
        )
        raise SystemExit(77)

    if os.name != "nt":
        return

    # Ensure "spawn" is used (Windows default), but force to avoid surprises.
    try:
        mp.set_start_method("spawn", force=True)
    except Exception:
        pass

    # Pin mp executable ASAP. Use both mp and multiprocessing.spawn fallbacks.
    try:
        if hasattr(mp, "set_executable"):
            mp.set_executable(str(expected))  # type: ignore[attr-defined]
        else:
            import multiprocessing.spawn as mps  # type: ignore
            if hasattr(mps, "set_executable"):
                mps.set_executable(str(expected))  # type: ignore[attr-defined]
    except Exception as e:
        sys.stderr.write(f"STOP Could not pin multiprocessing executable: {e}\n")
        raise SystemExit(79)

    # Verify pin stuck
    try:
        if hasattr(mp, "get_executable"):
            got = Path(str(mp.get_executable())).resolve()  # type: ignore[attr-defined]
        else:
            import multiprocessing.spawn as mps  # type: ignore
            got = Path(str(mps.get_executable())).resolve()  # type: ignore[attr-defined]
    except Exception as e:
        sys.stderr.write(f"STOP Could not read multiprocessing executable: {e}\n")
        raise SystemExit(80)

    if got != expected:
        sys.stderr.write(
            "STOP multiprocessing executable mismatch after pin (IMPORT-TIME):\n"
            f"  get_executable() = {got}\n"
            f"  expected         = {expected}\n"
        )
        raise SystemExit(81)


_bootstrap_hard_gate_and_pin()

# --- PHASE8_IMPORT_PATH_SHIM ---
import os as _os
import sys as _sys
from pathlib import Path as _Path

_ROOT = _Path(__file__).resolve().parents[2]
if str(_ROOT) not in _sys.path:
    _sys.path.insert(0, str(_ROOT))

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
    """Avoid Windows cp1252 stdout crashes from emoji/unicode in logging."""
    try:
        return s.encode("ascii", errors="replace").decode("ascii", errors="ignore")
    except Exception:
        return "MESSAGE_ENCODING_ERROR"


# ---------------------------------------------------------------------------
# ROOT + dotenv
# ---------------------------------------------------------------------------

def _resolve_root() -> Path:
    try:
        from app.core.config import settings  # type: ignore
        return Path(settings.ROOT)  # type: ignore
    except Exception:
        return Path(__file__).resolve().parents[2]


def _expected_venv_python(root: Path) -> Path:
    """
    Canonical runtime python for this repo. Direct-base mode is allowed on Windows
    when the repo venv is injected via environment.
    Windows: <ROOT>\\.venv\\Scripts\\python.exe
    """
    return _bootstrap_expected_runtime_python(root)


def _hard_gate_venv_interpreter(root: Path, log) -> Path:
    """
    HARD STOP if supervisor is not running under the expected repo runtime python.
    Returns expected runtime python path.
    (This is still kept, but the real enforcement is import-time bootstrap.)
    """
    expected = _expected_venv_python(root)
    actual = _current_python_executable()

    if not expected.exists():
        msg = f"STOP Missing expected runtime python: {expected}"
        log.error(_ascii_safe(msg))
        raise SystemExit(msg)

    if not _running_in_expected_runtime(root, expected):
        msg = (
            f"STOP Interpreter HARD GATE FAIL: sys.executable={actual} "
            f"!= expected runtime python={expected}; sys.prefix={sys.prefix}; "
            f"VIRTUAL_ENV={os.getenv('VIRTUAL_ENV', '')}; "
            f"FLASHBACK_DIRECT_BASE_PYTHON={os.getenv('FLASHBACK_DIRECT_BASE_PYTHON', '')}"
        )
        log.error(_ascii_safe(msg))
        raise SystemExit(msg)

    log.info("Interpreter HARD GATE PASS: sys.executable == expected runtime python (%s)", str(expected))
    return expected


def _mp_get_executable() -> Path:
    """
    Python version / platform compatible mp executable getter.
    - Some builds do NOT expose multiprocessing.get_executable (your case).
    - On Windows, the canonical getter is multiprocessing.spawn.get_executable().
    """
    if hasattr(mp, "get_executable"):
        got = mp.get_executable()  # type: ignore[attr-defined]
        return Path(str(got)).resolve()

    import multiprocessing.spawn as mps  # type: ignore
    if hasattr(mps, "get_executable"):
        got = mps.get_executable()  # type: ignore[attr-defined]
        return Path(str(got)).resolve()

    raise AttributeError("No supported get_executable found (multiprocessing or multiprocessing.spawn)")


def _mp_set_executable(expected_py: Path) -> None:
    """
    Python version / platform compatible mp executable setter.
    Prefer multiprocessing.set_executable; fall back to multiprocessing.spawn.set_executable if needed.
    """
    if hasattr(mp, "set_executable"):
        mp.set_executable(str(expected_py))  # type: ignore[attr-defined]
        return

    import multiprocessing.spawn as mps  # type: ignore
    if hasattr(mps, "set_executable"):
        mps.set_executable(str(expected_py))  # type: ignore[attr-defined]
        return

    raise AttributeError("No supported set_executable found (multiprocessing or multiprocessing.spawn)")


def _pin_multiprocessing_executable(expected_py: Path, log) -> None:
    """
    HARD pin spawn executable. If this fails or doesn't stick, STOP.
    This is the key to preventing runtime drift.
    """
    if os.name != "nt":
        log.info("Skipping multiprocessing executable pin on non-Windows platform (%s)", os.name)
        return

    try:
        _mp_set_executable(expected_py)
        log.info("Pinned multiprocessing executable (expected runtime python): %s", str(expected_py))
    except Exception as e:
        msg = f"STOP Could not pin multiprocessing executable to expected runtime python: {e}"
        log.error(_ascii_safe(msg))
        raise SystemExit(msg)

    # Hard verify
    try:
        got_path = _mp_get_executable()
    except Exception as e:
        msg = f"STOP Could not read multiprocessing executable: {e}"
        log.error(_ascii_safe(msg))
        raise SystemExit(msg)

    if got_path != expected_py.resolve():
        msg = (
            "STOP multiprocessing executable mismatch after pin. "
            f"get_executable()={got_path} expected={expected_py.resolve()}"
        )
        log.error(_ascii_safe(msg))
        raise SystemExit(msg)

    log.info("MP EXECUTABLE (get_executable) = %s", str(got_path))


def _load_env_file(root: Path, log) -> Dict[str, str]:
    """
    Load .env into process env, and return dotenv_values (file-first behavior).

    IMPORTANT:
    - If ORCH_ENV_LOADED=1, orchestrator already loaded .env and exported vars.
      In that case, do NOT load dotenv here (prevents double-load & surprises).
    """
    if os.getenv("ORCH_ENV_LOADED", "").strip() == "1":
        log.info("ORCH_ENV_LOADED=1 -> skipping dotenv load; relying on OS environment only.")
        return {}

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
    """Best-effort write into ops_snapshot.json. Never break supervisor if ops fails."""
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


def _file_first_str(env_file_vars: Dict[str, str], name: str, default: str = "") -> str:
    if name in env_file_vars:
        return str(env_file_vars[name] or "").strip()
    return str(os.getenv(name, default) or "").strip()


# ---------------------------------------------------------------------------
# HARD GATE: Config validation (robust import + file fallback)
# ---------------------------------------------------------------------------

def _run_validator_by_path(root: Path, log, expected_py: Path) -> int:
    """Fallback: run validate_config.py by file path using expected runtime python."""
    candidate = root / "app" / "tools" / "validate_config.py"
    if not candidate.exists():
        log.error("STOP Config validation missing: %s", candidate)
        return 2

    try:
        p = subprocess.run(
            [str(expected_py), str(candidate)],
            cwd=str(root),
            env=dict(os.environ),
            capture_output=True,
            text=True,
        )
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


def _hard_gate_validate_config(root: Path, log, send_tg, expected_py: Path) -> bool:
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

        rc = _run_validator_by_path(root, log, expected_py)
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

    accounts = []

    accounts_list = cfg.get("accounts")
    if isinstance(accounts_list, list):
        accounts = [acc for acc in accounts_list if isinstance(acc, dict)]
    else:
        for key, value in cfg.items():
            if key in ("version", "notes", "legacy"):
                continue
            if not isinstance(value, dict):
                continue
            entry = dict(value)
            entry.setdefault("account_label", str(key))
            accounts.append(entry)

    for acc in accounts:
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

    log.info("subaccounts.yaml: %s not found; default allow.", label)
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


def _call_entry(log, module, bot_name: str) -> None:
    import inspect
    import asyncio

    for fn_name in ("main", "loop", "run"):
        fn = getattr(module, fn_name, None)
        if callable(fn):
            log.info("%s entry: %s.%s()", bot_name, module.__name__, fn_name)
            res = fn()
            # If the entrypoint is async, run it properly.
            if inspect.iscoroutine(res):
                asyncio.run(res)
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


def _run_executor_v2() -> None:
    log = _get_logger()
    _, _, alert_bot_error = _load_common(log)
    try:
        mod = _import_first(log, ["app.bots.executor_v2"])
        _call_entry(log, mod, "executor_v2")
    except Exception as e:
        alert_bot_error("executor_v2", f"import/runtime error: {e}", "ERROR")


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
        # IMPORTANT:
        # Prefer REAL recorder (bots) first. The ai version is a stub/no-op in this repo.
        mod = _import_first(log, ["app.bots.trade_outcome_recorder", "app.ai.trade_outcome_recorder"])

        # Bulletproof rail: refuse to run the stub silently.
        if mod.__name__ == "app.ai.trade_outcome_recorder":
            allow = os.getenv("ALLOW_OUTCOME_STUB", "").strip().lower() in ("1", "true", "yes", "y", "on")
            if not allow:
                raise RuntimeError(
                    "Refusing STUB trade_outcome_recorder import (app.ai.trade_outcome_recorder). "
                    "Expected app.bots.trade_outcome_recorder. "
                    "If you *really* want stub behavior, set ALLOW_OUTCOME_STUB=1."
                )
            log.warning("ALLOW_OUTCOME_STUB=1 -> running STUB outcomes recorder (no-op).")

        _call_entry(log, mod, "trade_outcomes")
    except Exception as e:
        alert_bot_error("trade_outcomes", f"import/runtime error (optional): {e}", "WARN")


def _run_ai_events_spine() -> None:
    log = _get_logger()
    _, _, alert_bot_error = _load_common(log)
    try:
        mod = _import_first(log, ["app.ai.ai_events_spine"])
        _call_entry(log, mod, "main")
    except Exception as e:
        alert_bot_error("ai_events_spine", f"import/runtime error (optional): {e}", "WARN")


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
    def __init__(self, name: str, enabled: bool, command_parts: List[str]) -> None:
        self.name = name
        self.enabled = enabled
        self.command_parts = list(command_parts)
        self.process: Optional[subprocess.Popen[str]] = None
        self.stdout_handle: Optional[Any] = None
        self.stderr_handle: Optional[Any] = None
        self.stdout_path: Optional[Path] = None
        self.stderr_path: Optional[Path] = None
        self.restart_count: int = 0
        self.last_restart_ms: int = 0
        self.last_exitcode: Optional[int] = None
        self.last_reason: str = ""


def _build_worker_specs(env_file_vars: Dict[str, str]) -> Dict[str, WorkerSpec]:
    ws = _file_first_bool(env_file_vars, "AI_STACK_ENABLE_WS_SWITCHBOARD", "true")
    tp = _file_first_bool(env_file_vars, "AI_STACK_ENABLE_TP_SL_MANAGER", "true")
    execv2 = _file_first_bool(env_file_vars, "AI_STACK_ENABLE_EXECUTOR_V2", "true")
    pilot = _file_first_bool(env_file_vars, "AI_STACK_ENABLE_AI_PILOT", "true")
    router = _file_first_bool(env_file_vars, "AI_STACK_ENABLE_AI_ACTION_ROUTER", "true")
    journal = _file_first_bool(env_file_vars, "AI_STACK_ENABLE_AI_JOURNAL", "false")
    risk = _file_first_bool(env_file_vars, "AI_STACK_ENABLE_RISK_DAEMON", "false")
    outcomes = _file_first_bool(env_file_vars, "AI_STACK_ENABLE_TRADE_OUTCOMES", "true")
    spine = _file_first_bool(env_file_vars, "AI_STACK_ENABLE_AI_EVENTS_SPINE", "true")
    paper = _file_first_bool_alias(env_file_vars, "AI_STACK_ENABLE_PAPER_PRICE_FEEDER", "AI_STACK_ENABLE_PAPER_TICK_DAEMON", "false")

    return {
        "ws_switchboard": WorkerSpec("ws_switchboard", ws, ["-u", "-m", "app.core.ws_switchboard"]),
        "executor_v2": WorkerSpec("executor_v2", execv2, ["-u", "-m", "app.bots.executor_v2"]),
        "tp_sl_manager": WorkerSpec("tp_sl_manager", tp, ["-u", "-m", "app.bots.tp_sl_manager"]),
        "ai_pilot": WorkerSpec("ai_pilot", pilot, ["-u", "-m", "app.bots.ai_pilot"]),
        "ai_action_router": WorkerSpec(
            "ai_action_router",
            router,
            [
                "-u",
                "-c",
                "from app.core.ai_action_router import execsignal_queue_router_main as _fb_run; _fb_run()",
            ],
        ),
        "ai_journal": WorkerSpec(
            "ai_journal",
            journal,
            [
                "-u",
                "-c",
                "from app.bots.supervisor_ai_stack import _run_ai_journal as _fb_run; _fb_run()",
            ],
        ),
        "risk_daemon": WorkerSpec("risk_daemon", risk, ["-u", "-m", "app.bots.risk_daemon"]),
        "trade_outcomes": WorkerSpec("trade_outcomes", outcomes, ["-u", "-m", "app.bots.trade_outcome_recorder"]),
        "ai_events_spine": WorkerSpec("ai_events_spine", spine, ["-u", "-m", "app.ai.ai_events_spine"]),
        "paper_price_feeder": WorkerSpec("paper_price_feeder", paper, ["-u", "-m", "app.sim.paper_price_feeder"]),
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

def _worker_is_alive(spec: WorkerSpec) -> bool:
    return spec.process is not None and spec.process.poll() is None


def _worker_pid(spec: WorkerSpec) -> Optional[int]:
    return spec.process.pid if spec.process is not None else None


def _worker_command(spec: WorkerSpec, expected_py: Path) -> List[str]:
    return [str(expected_py), *spec.command_parts]


def _worker_logs_dir(account_label: str) -> Path:
    logs_dir = _ROOT / "state" / "orchestrator_logs" / "workers" / str(account_label or "main")
    logs_dir.mkdir(parents=True, exist_ok=True)
    return logs_dir


def _close_worker_log_handles(spec: WorkerSpec) -> None:
    for handle_name in ("stdout_handle", "stderr_handle"):
        handle = getattr(spec, handle_name, None)
        try:
            if handle is not None:
                handle.flush()
                handle.close()
        except Exception:
            pass
        setattr(spec, handle_name, None)


def _start_worker(log, spec: WorkerSpec, expected_py: Path, account_label: str) -> None:
    if _worker_is_alive(spec):
        return

    if not expected_py.exists():
        msg = f"STOP Refusing to start worker {spec.name}: missing runtime python {expected_py}"
        log.error(_ascii_safe(msg))
        raise SystemExit(msg)

    _close_worker_log_handles(spec)
    worker_logs_dir = _worker_logs_dir(account_label)
    spec.stdout_path = worker_logs_dir / f"{spec.name}.stdout.log"
    spec.stderr_path = worker_logs_dir / f"{spec.name}.stderr.log"
    spec.stdout_handle = open(spec.stdout_path, "a", encoding="utf-8", errors="replace")
    spec.stderr_handle = open(spec.stderr_path, "a", encoding="utf-8", errors="replace")

    log.info("Starting worker %s ...", spec.name)
    log.info(
        "ABOUT TO SPAWN | sys.executable=%s | worker_python=%s | worker_stdout=%s | worker_stderr=%s",
        str(_current_python_executable()),
        str(expected_py),
        str(spec.stdout_path),
        str(spec.stderr_path),
    )

    p = subprocess.Popen(
        _worker_command(spec, expected_py),
        cwd=str(_ROOT),
        env=dict(os.environ),
        text=True,
        stdout=spec.stdout_handle,
        stderr=spec.stderr_handle,
    )
    spec.process = p
    log.info("Worker %s started with pid=%s", spec.name, p.pid)


def _stop_worker(log, spec: WorkerSpec) -> None:
    p = spec.process
    if p is None:
        _close_worker_log_handles(spec)
        return

    if p.poll() is not None:
        spec.process = None
        _close_worker_log_handles(spec)
        return

    log.info("Stopping worker %s (pid=%s) ...", spec.name, p.pid)
    try:
        p.terminate()
    except Exception:
        pass

    try:
        p.wait(timeout=10)
    except Exception:
        pass

    if p.poll() is None:
        try:
            p.kill()
        except Exception:
            pass

    spec.process = None
    _close_worker_log_handles(spec)
    log.info("Worker %s stopped.", spec.name)


def _supervisor_loop(root: Path, account_label: str, poll_seconds: int, env_file_vars: Dict[str, str], expected_py: Path) -> None:
    log = _get_logger()
    record_heartbeat, send_tg, alert_bot_error = _load_common(log)

    alert_min_sec = int(os.getenv("AI_STACK_ALERT_MIN_INTERVAL_SEC", "20") or "20")
    last_alert_ms = 0

    log.info("BOOT | ROOT=%s | ACCOUNT_LABEL=%s | poll=%ss | expected_py=%s", str(root), account_label, poll_seconds, str(expected_py))

    kill_switch_path = Path(_ROOT) / "state" / "KILL_SWITCH"
    if kill_switch_path.exists():
        msg = f"STOP KILL SWITCH ACTIVE: {kill_switch_path}"
        log.error(_ascii_safe(msg))
        _ops_write("supervisor_ai_stack", account_label, False, {"phase": "blocked", "reason": "kill_switch"})
        raise SystemExit(msg)

    specs = _build_worker_specs(env_file_vars)
    lane_mode = str(os.getenv("FB_MODE") or os.getenv("MODE") or "").strip().upper()
    fleet_risk_owner = _file_first_str(env_file_vars, "FLEET_RISK_OWNER_LABEL", "flashback01") or "flashback01"
    fleet_risk_owner = fleet_risk_owner.strip().lower()
    if specs["risk_daemon"].enabled and str(account_label or "").strip().lower() != fleet_risk_owner:
        specs["risk_daemon"].enabled = False
        log.info(
            "Disabling risk_daemon for label=%s; fleet risk owner is %s",
            account_label,
            fleet_risk_owner,
        )
    if lane_mode and lane_mode != "LIVE" and specs["trade_outcomes"].enabled:
        specs["trade_outcomes"].enabled = False
        log.info(
            "Disabling trade_outcomes for label=%s; lane mode is %s and paper lanes now write canonical outcomes directly.",
            account_label,
            lane_mode,
        )
    if not specs["paper_price_feeder"].enabled:
        log.info("paper_price_feeder disabled for label=%s; stub worker is no longer launched by default.", account_label)

    log.info(
        "Flags (file-first): WS=%s EXEC=%s TP/SL=%s PILOT=%s ROUTER=%s RISK=%s OUTCOMES=%s SPINE=%s PAPER_FEED=%s",
        specs["ws_switchboard"].enabled,
        specs["executor_v2"].enabled,
        specs["tp_sl_manager"].enabled,
        specs["ai_pilot"].enabled,
        specs["ai_action_router"].enabled,
        specs["risk_daemon"].enabled,
        specs["trade_outcomes"].enabled,
        specs["ai_events_spine"].enabled,
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
                if _worker_is_alive(spec):
                    log.info("Worker %s disabled -> stopping.", name)
                    _stop_worker(log, spec)
                _ops_write(f"worker_{name}", account_label, True, {"enabled": False, "state": "disabled"})
                continue

            alive = _worker_is_alive(spec)
            if not alive:
                if spec.process is not None:
                    spec.last_exitcode = spec.process.returncode
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

                _start_worker(log, spec, expected_py, account_label)

            alive = _worker_is_alive(spec)
            pid = _worker_pid(spec)
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
                    "stdout_log": str(spec.stdout_path) if spec.stdout_path else None,
                    "stderr_log": str(spec.stderr_path) if spec.stderr_path else None,
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
    log = _get_logger()
    root = _resolve_root()

    expected_py = _hard_gate_venv_interpreter(root, log)
    _pin_multiprocessing_executable(expected_py, log)

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

    if not _hard_gate_validate_config(root, log, send_tg, expected_py):
        _ops_write("supervisor_ai_stack", account_label, False, {"phase": "blocked", "reason": "config validation failed"})
        return

    try:
        _supervisor_loop(root, account_label, poll_seconds, env_file_vars, expected_py)
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
