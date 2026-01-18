#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Trade Outcome Recorder v2.6.0 (Source-aware: EXEC bus + PAPER ledger)

v2.6.0:
- NEW: source-aware ingestion
    LIVE      -> executions bus (ws_executions*.jsonl)
    PAPER/DRY -> paper ledger stream (env or auto-detect)
- NEW: PAPER_LEDGER_PATH env (absolute or relative-to-state)
- NEW: auto-detect newest paper stream file if PAPER_LEDGER_PATH not set
- Supports:
    - pass-through publish for already-built outcome.v1 lines
    - best-effort conversion for common paper fill/close shapes
- Keeps v2.5.2+PATCH fixes (account attribution, flat result payload support, dedupe, pending reconcile)
"""

from __future__ import annotations

import json
import os
import re
import time
from collections import deque
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

try:
    import orjson
except Exception:  # pragma: no cover
    orjson = None  # type: ignore


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

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


log = get_logger("trade_outcome_recorder")


# ---------------------------------------------------------------------------
# Core config / paths
# ---------------------------------------------------------------------------

try:
    from app.core.config import settings
    ROOT: Path = settings.ROOT  # type: ignore[attr-defined]
except Exception:
    ROOT = Path(__file__).resolve().parents[2]

STATE_DIR: Path = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)


def _account_label() -> str:
    s = (os.getenv("ACCOUNT_LABEL") or "").strip()
    return s if s else "main"


def _is_main_label(label: str) -> bool:
    return (label or "").strip().lower() in ("main", "primary")


ACCOUNT_LABEL: str = _account_label()


def _env_path(name: str, default: str) -> Path:
    v = os.getenv(name)
    s = (v or "").strip()
    p = Path(s) if s else Path(default)
    if not p.is_absolute():
        p = STATE_DIR / p
    return p


def _default_exec_bus_path_for_label(label: str) -> Path:
    """
    Default exec bus paths:
      - main/primary -> state/ws_executions.jsonl (back-compat truth)
      - subs         -> state/ws_executions_<label>.jsonl
    """
    lab = (label or "").strip().lower()
    if _is_main_label(lab):
        return STATE_DIR / "ws_executions.jsonl"
    return STATE_DIR / f"ws_executions_{lab}.jsonl"


# Paths (env-overridable for per-account isolation)
#
# EXEC path precedence:
# 1) EXEC_BUS_PATH
# 2) EXECUTIONS_BUS_PATH
# 3) EXECUTIONS_PATH
# 4) default:
#      main/primary -> state/ws_executions.jsonl
#      subs         -> state/ws_executions_<ACCOUNT_LABEL>.jsonl
_default_exec = _default_exec_bus_path_for_label(ACCOUNT_LABEL)
_default_exec = _env_path("EXECUTIONS_PATH", str(_default_exec))
_default_exec = _env_path("EXECUTIONS_BUS_PATH", str(_default_exec))
EXEC_BUS_PATH: Path = _env_path("EXEC_BUS_PATH", str(_default_exec))

# Cursor default: per-account by default
CURSOR_PATH: Path = _env_path("TRADE_OUTCOME_CURSOR_PATH", f"trade_outcome_recorder_{ACCOUNT_LABEL}.cursor")

# Canonical AI events dir default MUST match spine:
#   state/ai_events
AI_EVENTS_DIR: Path = _env_path("AI_EVENTS_DIR", "ai_events")
AI_EVENTS_DIR.mkdir(parents=True, exist_ok=True)

# Ensure downstream modules that read env agree on the same AI_EVENTS_DIR (fail-soft)
try:
    if not (os.getenv("AI_EVENTS_DIR") or "").strip():
        os.environ["AI_EVENTS_DIR"] = str(AI_EVENTS_DIR)
except Exception:
    pass

PENDING_SETUPS_PATH: Path = AI_EVENTS_DIR / "pending_setups.json"
PENDING_SETUPS_LOCK: Path = PENDING_SETUPS_PATH.with_suffix(PENDING_SETUPS_PATH.suffix + ".lock")


# ---------------------------------------------------------------------------
# Heartbeat + alert helpers
# ---------------------------------------------------------------------------

try:
    from app.core.flashback_common import record_heartbeat, alert_bot_error  # type: ignore
except Exception:  # pragma: no cover
    def record_heartbeat(name: str) -> None:
        return None

    def alert_bot_error(bot_name: str, msg: str, level: str = "ERROR") -> None:
        if level.upper() == "ERROR":
            log.error("[%s] %s", bot_name, msg)
        else:
            log.warning("[%s] %s", bot_name, msg)


# ---------------------------------------------------------------------------
# Subs mapping: sub_uid → account_label
# ---------------------------------------------------------------------------

_SUB_UID_TO_LABEL: Dict[str, str] = {}

try:
    from app.core.subs import all_subs as load_subs  # type: ignore
    subs_cfg = load_subs()
    for lbl, cfg in (subs_cfg or {}).items():
        uid_raw = (
            cfg.get("sub_uid")
            or cfg.get("uid")
            or cfg.get("memberId")
            or cfg.get("user_id")
            or cfg.get("subAccountId")
        )
        if uid_raw is None:
            continue
        uid = str(uid_raw).strip()
        if uid:
            _SUB_UID_TO_LABEL[uid] = str(lbl)
except Exception:
    _SUB_UID_TO_LABEL = {}


# ---------------------------------------------------------------------------
# AI events (OutcomeRecord)
# ---------------------------------------------------------------------------

try:
    from app.ai.ai_events_spine import build_outcome_record, publish_ai_event  # type: ignore
except Exception as e:  # pragma: no cover
    raise RuntimeError(f"ai_events_spine is required for trade_outcome_recorder: {e}")


# ---------------------------------------------------------------------------
# File lock (Windows-friendly best-effort)
# ---------------------------------------------------------------------------

class _FileLock:
    def __init__(self, lock_path: Path, timeout_sec: float = 2.5) -> None:
        self.lock_path = lock_path
        self.timeout_sec = timeout_sec
        self._fh = None

    def __enter__(self):
        try:
            self.lock_path.parent.mkdir(parents=True, exist_ok=True)
            self._fh = open(self.lock_path, "a+b")
            try:
                import msvcrt
                start = time.time()
                while True:
                    try:
                        msvcrt.locking(self._fh.fileno(), msvcrt.LK_NBLCK, 1)
                        break
                    except OSError:
                        if (time.time() - start) >= self.timeout_sec:
                            break
                        time.sleep(0.02)
            except Exception:
                pass
        except Exception:
            self._fh = None
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if self._fh is None:
                return
            try:
                import msvcrt
                try:
                    self._fh.seek(0)
                    msvcrt.locking(self._fh.fileno(), msvcrt.LK_UNLCK, 1)
                except Exception:
                    pass
            except Exception:
                pass
            try:
                self._fh.close()
            except Exception:
                pass
        except Exception:
            return


# ---------------------------------------------------------------------------
# Cursor helpers
# ---------------------------------------------------------------------------

_CURSOR_INT_RE = re.compile(r"^\s*(\d+)", re.MULTILINE)


def _load_cursor() -> int:
    """Load cursor as a byte offset (extract first integer, else 0)."""
    if not CURSOR_PATH.exists():
        return 0
    try:
        txt = CURSOR_PATH.read_text(encoding="utf-8", errors="ignore")
        m = _CURSOR_INT_RE.search(txt or "")
        if not m:
            return 0
        return int(m.group(1))
    except Exception:
        return 0


def _save_cursor(pos: int) -> None:
    try:
        CURSOR_PATH.write_text(str(int(pos)), encoding="utf-8")
    except Exception as e:
        log.warning("failed to save cursor=%s: %r", pos, e)


# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------

def _now_ms() -> int:
    return int(time.time() * 1000)


# ---------------------------------------------------------------------------
# Mode detection
# ---------------------------------------------------------------------------

def _env_mode() -> str:
    m = (os.getenv("FB_MODE") or os.getenv("MODE") or "").strip()
    return m.upper() if m else ""


def _mode_from_hint(hint: Dict[str, Any]) -> str:
    payload = hint.get("payload") if isinstance(hint.get("payload"), dict) else {}
    extra = payload.get("extra") if isinstance(payload.get("extra"), dict) else {}
    mode = extra.get("mode")
    if isinstance(mode, str) and mode.strip():
        return mode.strip().upper()
    env_mode = _env_mode()
    if env_mode:
        return env_mode
    return "LIVE"


def _is_paper_like(mode: str) -> bool:
    m = (mode or "").strip().upper()
    return m in ("PAPER", "DRY", "LEARN_DRY", "BACKTEST", "SIM", "SIMULATED")


# ---------------------------------------------------------------------------
# Pending setups helpers
# ---------------------------------------------------------------------------

_PENDING_CACHE: Dict[str, Any] = {}
_PENDING_LAST_LOAD_MS: int = 0


def _read_json_file_utf8_sig(path: Path) -> Dict[str, Any]:
    try:
        txt = path.read_text(encoding="utf-8-sig")
        data = json.loads(txt or "{}")
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _atomic_write_json(path: Path, data: Dict[str, Any]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(
            json.dumps(data, ensure_ascii=False, separators=(",", ":"), sort_keys=True),
            encoding="utf-8",
        )
        tmp.replace(path)
    except Exception:
        return


def _load_pending_setups_cached(max_age_ms: int = 1500) -> Dict[str, Any]:
    global _PENDING_CACHE, _PENDING_LAST_LOAD_MS

    now = _now_ms()
    if (now - _PENDING_LAST_LOAD_MS) < max_age_ms and _PENDING_CACHE:
        return _PENDING_CACHE

    _PENDING_LAST_LOAD_MS = now

    if not PENDING_SETUPS_PATH.exists():
        _PENDING_CACHE = {}
        return _PENDING_CACHE

    _PENDING_CACHE = _read_json_file_utf8_sig(PENDING_SETUPS_PATH)
    return _PENDING_CACHE


def _setup_hint_for_any_id(*ids: str) -> Optional[Dict[str, Any]]:
    pending = _load_pending_setups_cached()
    for k in ids:
        if not k:
            continue
        hit = pending.get(str(k))
        if isinstance(hit, dict):
            return hit
    return None


def _setup_ts_ms(hint: Dict[str, Any]) -> Optional[int]:
    try:
        ts = hint.get("ts")
        if ts is None:
            return None
        return int(ts)
    except Exception:
        return None


def _setup_features(hint: Dict[str, Any]) -> Dict[str, Any]:
    payload = hint.get("payload") if isinstance(hint.get("payload"), dict) else {}
    feats = payload.get("features") if isinstance(payload.get("features"), dict) else {}
    return feats


def _setup_extra(hint: Dict[str, Any]) -> Dict[str, Any]:
    payload = hint.get("payload") if isinstance(hint.get("payload"), dict) else {}
    extra = payload.get("extra") if isinstance(payload.get("extra"), dict) else {}
    return extra


def _setup_fp(hint: Dict[str, Any]) -> Optional[str]:
    feats = _setup_features(hint)
    fp = feats.get("setup_fingerprint")
    if isinstance(fp, str) and fp.strip():
        return fp.strip()
    return None


def _setup_fp_features(hint: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    feats = _setup_features(hint)
    fpf = feats.get("setup_fingerprint_features")
    if isinstance(fpf, dict):
        return fpf
    return None


def _is_setup_aborted(hint: Dict[str, Any]) -> bool:
    extra = _setup_extra(hint)
    for k in ("aborted", "is_aborted", "abort", "cancelled", "canceled"):
        v = extra.get(k)
        if isinstance(v, bool) and v:
            return True
        if isinstance(v, str) and v.strip().lower() in ("true", "1", "yes", "aborted"):
            return True
    v2 = hint.get("status") or hint.get("final_status")
    if isinstance(v2, str) and v2.strip().upper() == "ABORTED":
        return True
    return False


# ---------------------------------------------------------------------------
# Execution row normalization (LIVE)
# ---------------------------------------------------------------------------

def _iter_execution_rows(msg: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    """
    Normalizes multiple possible execution message shapes into row dicts.

    HARD RULE:
    - If a row is missing account_label, stamp it with this recorder's ACCOUNT_LABEL.
    """
    wrapper_account_label = msg.get("account_label") or msg.get("label") or ""
    default_label = str(wrapper_account_label).strip() if str(wrapper_account_label).strip() else ACCOUNT_LABEL

    if "data" in msg:
        data = msg["data"]
        if isinstance(data, dict):
            row = dict(data)
            if not row.get("account_label"):
                row["account_label"] = default_label
            yield row
            return
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    row = dict(item)
                    if not row.get("account_label"):
                        row["account_label"] = default_label
                    yield row
            return

    if msg.get("topic") == "execution" and isinstance(msg.get("result"), dict):
        res = msg["result"]

        data = res.get("data")
        if isinstance(data, dict):
            row = dict(data)
            if not row.get("account_label"):
                row["account_label"] = default_label
            yield row
            return
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    row = dict(item)
                    if not row.get("account_label"):
                        row["account_label"] = default_label
                    yield row
            return

        if isinstance(res.get("symbol"), (str,)) and (
            ("execPrice" in res) or ("execQty" in res) or ("execValue" in res) or ("execId" in res) or ("orderLinkId" in res)
        ):
            row = dict(res)
            if not row.get("account_label"):
                row["account_label"] = default_label
            yield row
            return

    if "symbol" in msg and ("execPrice" in msg or "execQty" in msg or "execValue" in msg):
        row = dict(msg)
        if not row.get("account_label"):
            row["account_label"] = default_label
        yield row
        return

    return


# ---------------------------------------------------------------------------
# PAPER stream selection + normalization
# ---------------------------------------------------------------------------

_PAPER_GLOB_PATTERNS = (
    "paper_ledger*.jsonl",
    "paper*.jsonl",
    "*paper*.jsonl",
    "*ledger*.jsonl",
    "*fills*.jsonl",
    "*fill*.jsonl",
    "*broker*.jsonl",
    "*sim*.jsonl",
    "*dry*.jsonl",
)


def _paper_stream_path() -> Optional[Path]:
    """
    Choose paper stream path.
    Precedence:
      1) PAPER_LEDGER_PATH env
      2) auto-detect newest match in state/ by patterns
    """
    envp = (os.getenv("PAPER_LEDGER_PATH") or "").strip()
    if envp:
        p = Path(envp)
        if not p.is_absolute():
            p = STATE_DIR / p
        return p

    # Auto-detect (newest)
    candidates: list[Path] = []
    try:
        for pat in _PAPER_GLOB_PATTERNS:
            candidates.extend(STATE_DIR.glob(pat))
    except Exception:
        candidates = []

    # Filter: must be file, must exist, must be jsonl, must have non-zero size (or allow 0 if it's actively being written)
    filtered: list[Path] = []
    for p in candidates:
        try:
            if not p.exists() or not p.is_file():
                continue
            if p.suffix.lower() != ".jsonl":
                continue
            filtered.append(p)
        except Exception:
            continue

    if not filtered:
        return None

    filtered.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    return filtered[0]


def _looks_like_outcome_v1(obj: Dict[str, Any]) -> bool:
    sv = obj.get("schema_version") or obj.get("extra", {}).get("schema_version")
    if isinstance(sv, str) and "outcome" in sv.lower():
        # loose check
        return True
    # spine events often include explicit fields
    return all(k in obj for k in ("trade_id", "symbol", "account_label")) and ("pnl_usd" in obj or "extra" in obj)


def _normalize_ts_ms(ts_raw: Any) -> int:
    try:
        v = int(str(ts_raw))
    except Exception:
        return _now_ms()
    if v > 0 and v < 10_000_000_000:
        return v * 1000
    return v


def _decimal(raw: Any, default: str = "0") -> Decimal:
    try:
        if raw in (None, "", "null"):
            return Decimal(default)
        return Decimal(str(raw))
    except Exception:
        return Decimal(default)


def _strip_bom(s: str) -> str:
    return s.lstrip("\ufeff")


def _loads_json(line: str) -> Optional[Dict[str, Any]]:
    try:
        line = _strip_bom(line)
        if orjson is not None:
            obj = orjson.loads(line.encode("utf-8", errors="ignore"))
            return obj if isinstance(obj, dict) else None
        obj = json.loads(line)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _paper_trade_id(obj: Dict[str, Any]) -> str:
    for k in ("trade_id", "orderLinkId", "order_link_id", "orderId", "order_id", "id"):
        v = obj.get(k)
        if v not in (None, ""):
            return str(v)
    return f"paper_{_now_ms()}"


def _paper_account_label(obj: Dict[str, Any]) -> str:
    lab = obj.get("account_label") or obj.get("label")
    if lab:
        return str(lab)
    uid = obj.get("sub_uid") or obj.get("subAccountId") or obj.get("memberId") or obj.get("uid")
    if uid not in (None, ""):
        return _SUB_UID_TO_LABEL.get(str(uid), f"sub_{uid}")
    return ACCOUNT_LABEL


def _paper_strategy(obj: Dict[str, Any]) -> str:
    for k in ("strategy", "strategy_name", "strategyName"):
        v = obj.get(k)
        if v not in (None, ""):
            return str(v)
    return "unknown_strategy"


def _paper_timeframe(obj: Dict[str, Any]) -> str:
    for k in ("timeframe", "tf"):
        v = obj.get(k)
        if v not in (None, ""):
            return str(v)
    return "UNKNOWN"


def _paper_symbol(obj: Dict[str, Any]) -> Optional[str]:
    sym = obj.get("symbol") or obj.get("instrument") or obj.get("s")
    if sym:
        return str(sym)
    return None


def _paper_is_terminal(obj: Dict[str, Any]) -> bool:
    v = obj.get("is_terminal")
    if isinstance(v, bool):
        return v
    status = (obj.get("final_status") or obj.get("status") or obj.get("event") or obj.get("type") or "").strip().upper()
    if status in ("CLOSED", "CLOSE", "EXIT", "TERMINAL", "FINAL", "TP", "SL", "LIQ", "LIQUIDATED"):
        return True
    # if it has realized pnl + close_reason, treat as terminal
    if obj.get("pnl_usd") not in (None, "", "null") and (obj.get("close_reason") or obj.get("exit_reason")):
        return True
    return False


def _build_outcome_from_paper_obj(obj: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Best-effort conversion from a paper event to an OutcomeRecord.
    If obj already looks like an outcome event, return it (pass-through).
    """
    if _looks_like_outcome_v1(obj):
        return obj

    trade_id = _paper_trade_id(obj)
    symbol = _paper_symbol(obj)
    if not symbol:
        return None

    account_label = _paper_account_label(obj)
    strategy_name = _paper_strategy(obj)
    timeframe = _paper_timeframe(obj)

    # pnl_usd: prefer explicit
    pnl_raw = obj.get("pnl_usd")
    if pnl_raw in (None, "", "null"):
        pnl_raw = obj.get("pnl") or obj.get("realized_pnl") or obj.get("closedPnl") or obj.get("closed_pnl")
    try:
        pnl_usd = float(pnl_raw) if pnl_raw not in (None, "", "null") else 0.0
    except Exception:
        pnl_usd = 0.0

    is_terminal = _paper_is_terminal(obj)

    # If not terminal, treat as fill event and try to compute cashflow-like pnl
    side = (obj.get("side") or "").strip()
    px = _decimal(obj.get("price") or obj.get("exec_price") or obj.get("execPrice") or obj.get("fill_price") or "0")
    qty = _decimal(obj.get("qty") or obj.get("exec_qty") or obj.get("execQty") or obj.get("fill_qty") or "0")
    fee = _decimal(obj.get("fee") or obj.get("exec_fee") or obj.get("execFee") or "0")

    ts_raw = obj.get("ts_exec_ms") or obj.get("ts") or obj.get("timestamp") or obj.get("time") or obj.get("created_ms")
    ts_exec_ms = _normalize_ts_ms(ts_raw)

    exit_reason = obj.get("close_reason") or obj.get("exit_reason") or obj.get("reason") or obj.get("event") or obj.get("type") or "paper_event"

    # Fill-like cashflow (only if not terminal and pnl not explicitly provided)
    if not is_terminal and (pnl_raw in (None, "", "null")) and px > 0 and qty > 0:
        val = px * qty
        if side.lower() == "buy":
            pnl_usd = float(-(val + fee))
        elif side.lower() == "sell":
            pnl_usd = float(val - fee)

    # Terminal-only fields (we do NOT compute r here without reliable risk_usd)
    r_multiple: Optional[float] = None
    win: Optional[bool] = None

    try:
        evt = build_outcome_record(
            trade_id=str(trade_id),
            symbol=str(symbol),
            account_label=str(account_label),
            strategy=str(strategy_name),
            pnl_usd=float(pnl_usd),
            r_multiple=r_multiple,
            win=win,
            exit_reason=str(exit_reason),
            extra={
                "schema_version": "paper_outcome_v2_6_0",
                "final_status": "TERMINAL" if is_terminal else "FILL_EVENT",
                "is_terminal": bool(is_terminal),
                "mode": "PAPER",

                "timeframe": str(timeframe),
                "side": side,
                "exec_price": float(px) if px is not None else None,
                "exec_qty": float(qty) if qty is not None else None,
                "exec_fee": float(fee) if fee is not None else None,
                "ts_exec_ms": int(ts_exec_ms),

                "raw": obj,
            },
        )
        if isinstance(evt, dict):
            evt["timeframe"] = str(timeframe)
    except Exception as e:
        log.warning("failed to build outcome_record from paper obj for %s: %r", symbol, e)
        return None

    return evt


# ---------------------------------------------------------------------------
# Synthetic terminal emission (pending reconcile) - unchanged
# ---------------------------------------------------------------------------

def _emit_terminal_outcome_for_pending(trade_id: str, hint: Dict[str, Any], final_status: str, exit_reason: str) -> bool:
    symbol = hint.get("symbol") or "UNKNOWN"
    account_label = hint.get("account_label") or "main"
    strategy_name = hint.get("strategy_name") or hint.get("strategy") or "unknown_strategy"
    timeframe = hint.get("timeframe") or "UNKNOWN"

    feats = _setup_features(hint)
    risk_usd = feats.get("risk_usd")
    mfe = feats.get("mfe") or feats.get("max_favorable_excursion")
    mae = feats.get("mae") or feats.get("max_adverse_excursion")

    setup_fingerprint = _setup_fp(hint)
    setup_fp_features = _setup_fp_features(hint)

    mode = _mode_from_hint(hint)

    pnl_usd = 0.0
    r_multiple: Optional[float] = None
    win: Optional[bool] = None

    try:
        evt = build_outcome_record(
            trade_id=str(trade_id),
            symbol=str(symbol),
            account_label=str(account_label),
            strategy=str(strategy_name),
            pnl_usd=float(pnl_usd),
            r_multiple=r_multiple,
            win=win,
            exit_reason=exit_reason,
            extra={
                "schema_version": "synthetic_terminal_v2_6_0",
                "final_status": final_status,
                "is_terminal": True,
                "is_synthetic_terminal": True,
                "mode": mode,
                "timeframe": str(timeframe),

                "risk_usd": float(risk_usd) if risk_usd not in (None, "", "null") else None,
                "mfe": mfe,
                "mae": mae,
                "setup_fingerprint": setup_fingerprint,
                "setup_fingerprint_features": setup_fp_features,
                "setup_snapshot": hint,
            },
        )
        if isinstance(evt, dict):
            evt["timeframe"] = str(timeframe)
        publish_ai_event(evt)
        return True
    except Exception as e:
        log.warning("failed to publish synthetic terminal outcome for %s: %r", trade_id, e)
        return False


def _reconcile_pending_setups(timeout_minutes: int) -> int:
    if not PENDING_SETUPS_PATH.exists():
        return 0

    now = _now_ms()
    timeout_ms = int(timeout_minutes) * 60_000
    published = 0

    with _FileLock(PENDING_SETUPS_LOCK, timeout_sec=float(os.getenv("PENDING_SETUPS_LOCK_TIMEOUT_SEC", "2.5"))):
        pending = _read_json_file_utf8_sig(PENDING_SETUPS_PATH)
        if not pending:
            return 0

        changed = False

        for trade_id, hint in list(pending.items()):
            if not isinstance(hint, dict):
                continue

            ts = _setup_ts_ms(hint)
            if ts is None:
                continue

            age = now - ts

            if _is_setup_aborted(hint):
                if _emit_terminal_outcome_for_pending(str(trade_id), hint, "ABORTED", "ABORTED"):
                    published += 1
                    pending.pop(trade_id, None)
                    changed = True
                continue

            if age >= timeout_ms:
                if _emit_terminal_outcome_for_pending(str(trade_id), hint, "EXPIRED", f"EXPIRED_TIMEOUT_{timeout_minutes}m"):
                    published += 1
                    pending.pop(trade_id, None)
                    changed = True

        if changed:
            _atomic_write_json(PENDING_SETUPS_PATH, pending)
            global _PENDING_CACHE, _PENDING_LAST_LOAD_MS
            _PENDING_CACHE = dict(pending)
            _PENDING_LAST_LOAD_MS = _now_ms()

    return published


# ---------------------------------------------------------------------------
# Dedupe
# ---------------------------------------------------------------------------

_DEDUPE_MAX = 5000
_recent_ids = set()
_recent_ids_q: deque[str] = deque()


def _dedupe_id(key: str) -> bool:
    if not key:
        return False
    if key in _recent_ids:
        return False
    _recent_ids.add(key)
    _recent_ids_q.append(key)
    while len(_recent_ids_q) > _DEDUPE_MAX:
        old = _recent_ids_q.popleft()
        _recent_ids.discard(old)
    return True


# ---------------------------------------------------------------------------
# LIVE processing (exec bus)
# ---------------------------------------------------------------------------

def _exec_id_from_row(row: Dict[str, Any]) -> str:
    for k in ("execId", "exec_id", "executionId", "execution_id", "id"):
        v = row.get(k)
        if v not in (None, ""):
            return str(v)

    order_id = row.get("orderId") or row.get("order_id") or ""
    ts = row.get("execTime") or row.get("exec_time") or row.get("T") or row.get("ts") or ""
    sym = row.get("symbol") or ""
    px = row.get("execPrice") or row.get("price") or ""
    qty = row.get("execQty") or row.get("qty") or ""
    side = row.get("side") or ""

    if order_id or ts or sym:
        return f"fallback:{order_id}:{ts}:{sym}:{px}:{qty}:{side}"

    try:
        if orjson is not None:
            b = orjson.dumps(row, option=orjson.OPT_SORT_KEYS, default=str)
        else:
            b = json.dumps(row, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8", errors="ignore")
        return "hash:" + __import__("hashlib").sha256(b).hexdigest()
    except Exception:
        return f"hash:unknown:{_now_ms()}"


def _trade_id_from_row(row: Dict[str, Any]) -> str:
    trade_id = row.get("orderLinkId") or row.get("order_link_id") or row.get("orderId") or row.get("order_id")
    if trade_id:
        return str(trade_id)
    return f"exec_{row.get('symbol','UNKNOWN')}_{_now_ms()}"


def _resolve_hint_for_row(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    trade_id = str(row.get("orderLinkId") or row.get("order_link_id") or "") or ""
    order_id = str(row.get("orderId") or row.get("order_id") or "") or ""
    fallback_trade = _trade_id_from_row(row)
    return _setup_hint_for_any_id(trade_id, order_id, fallback_trade)


def _resolve_account_label(row: Dict[str, Any], hint: Optional[Dict[str, Any]]) -> str:
    label = row.get("account_label") or row.get("label") or row.get("account_label_slug") or ""
    if label:
        return str(label)

    sub_uid_raw = row.get("sub_uid") or row.get("subAccountId") or row.get("accountId") or row.get("subId") or row.get("uid") or row.get("memberId")
    if sub_uid_raw not in (None, ""):
        uid = str(sub_uid_raw)
        return _SUB_UID_TO_LABEL.get(uid, f"sub_{uid}")

    if hint:
        hl = hint.get("account_label")
        if hl:
            return str(hl)

    return ACCOUNT_LABEL


def _resolve_strategy_name(row: Dict[str, Any], hint: Optional[Dict[str, Any]]) -> str:
    strategy_name = row.get("strategy_name") or row.get("strategy") or row.get("strat_label") or ""
    if strategy_name:
        return str(strategy_name)
    if hint:
        hs = hint.get("strategy_name") or hint.get("strategy")
        if hs:
            return str(hs)
    return "unknown_strategy"


def _resolve_timeframe(row: Dict[str, Any], hint: Optional[Dict[str, Any]]) -> str:
    tf = row.get("timeframe") or row.get("tf") or ""
    if tf:
        return str(tf)
    if hint:
        ht = hint.get("timeframe")
        if ht:
            return str(ht)
    return "UNKNOWN"


def _resolve_symbol(row: Dict[str, Any], hint: Optional[Dict[str, Any]]) -> Optional[str]:
    sym = row.get("symbol")
    if sym:
        return str(sym)
    if hint:
        hs = hint.get("symbol")
        if hs:
            return str(hs)
    return None


def _extract_realized_pnl_usd(row: Dict[str, Any]) -> Optional[float]:
    for k in ("closedPnl", "closed_pnl", "execPnl", "exec_pnl", "realizedPnl", "realized_pnl", "pnl"):
        v = row.get(k)
        if v not in (None, "", "null"):
            try:
                return float(v)
            except Exception:
                continue
    return None


def _compute_fill_cashflow(side: str, exec_price: Decimal, exec_qty: Decimal, exec_value: Decimal, exec_fee: Decimal) -> Decimal:
    s = (side or "").strip().lower()
    if exec_value <= 0 and exec_price > 0 and exec_qty > 0:
        exec_value = exec_price * exec_qty
    if s == "buy":
        return -(exec_value + exec_fee)
    if s == "sell":
        return exec_value - exec_fee
    return Decimal("0")


def _build_outcome_from_exec_row(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    hint = _resolve_hint_for_row(row)

    trade_id = _trade_id_from_row(row)
    symbol = _resolve_symbol(row, hint)
    if not symbol:
        return None

    side = row.get("side")
    exec_price = _decimal(row.get("execPrice") or row.get("price"))
    exec_qty = _decimal(row.get("execQty") or row.get("qty"))
    exec_value = _decimal(row.get("execValue") or row.get("value"))
    exec_fee = _decimal(row.get("execFee") or row.get("fee"))

    if exec_qty <= 0:
        return None

    account_label = _resolve_account_label(row, hint)
    strategy_name = _resolve_strategy_name(row, hint)
    timeframe = _resolve_timeframe(row, hint)

    realized_pnl = _extract_realized_pnl_usd(row)
    pnl_kind = "realized_pnl" if realized_pnl is not None else "fill_cashflow"

    cashflow = _compute_fill_cashflow(
        side=str(side or ""),
        exec_price=exec_price,
        exec_qty=exec_qty,
        exec_value=exec_value,
        exec_fee=exec_fee,
    )
    pnl_usd = float(realized_pnl) if realized_pnl is not None else float(cashflow)

    ts_exec_raw = row.get("execTime") or row.get("exec_time") or row.get("T") or row.get("ts")
    ts_exec_ms = _normalize_ts_ms(ts_exec_raw)

    exec_type = row.get("execType") or row.get("exec_type") or row.get("type") or "execution"

    hintd = hint or {}
    feats = _setup_features(hintd)
    risk_usd = feats.get("risk_usd")

    setup_fingerprint = _setup_fp(hintd)
    setup_fp_features = _setup_fp_features(hintd)

    mode = _mode_from_hint(hintd)

    r_multiple: Optional[float] = None
    win: Optional[bool] = None

    setup_type = hintd.get("setup_type")
    ai_profile = hintd.get("ai_profile")

    mfe = feats.get("mfe") or feats.get("max_favorable_excursion")
    mae = feats.get("mae") or feats.get("max_adverse_excursion")

    try:
        event = build_outcome_record(
            trade_id=str(trade_id),
            symbol=str(symbol),
            account_label=str(account_label),
            strategy=str(strategy_name),
            pnl_usd=float(pnl_usd),
            r_multiple=r_multiple,
            win=win,
            exit_reason=str(exec_type),
            extra={
                "schema_version": "outcome_from_exec_v2_6_0",
                "final_status": "FILL_EVENT",
                "is_terminal": False,
                "pnl_kind": pnl_kind,
                "mode": mode,
                "timeframe": str(timeframe),

                "side": side,
                "exec_price": float(exec_price),
                "exec_qty": float(exec_qty),
                "exec_value": float(exec_value),
                "exec_fee": float(exec_fee),
                "cashflow_usd": float(cashflow),
                "realized_pnl_usd": float(realized_pnl) if realized_pnl is not None else None,
                "risk_usd": float(risk_usd) if risk_usd not in (None, "", "null") else None,
                "mfe": mfe,
                "mae": mae,
                "ts_exec_ms": ts_exec_ms,
                "setup_type_hint": setup_type,
                "ai_profile_hint": ai_profile,
                "setup_fingerprint": setup_fingerprint,
                "setup_fingerprint_features": setup_fp_features,
                "trade_id": str(trade_id),
                "account_label": str(account_label),
                "strategy_name": str(strategy_name),
                "raw": row,
            },
        )
        if isinstance(event, dict):
            event["timeframe"] = str(timeframe)
    except Exception as e:
        log.warning("failed to build outcome_record for %s: %r", symbol, e)
        return None

    return event


def _process_live_line(raw: bytes) -> int:
    try:
        line = raw.decode("utf-8", errors="replace")
    except Exception as e:
        log.warning("failed to decode exec bus line: %r", e)
        return 0

    line = _strip_bom(line).strip()
    if not line:
        return 0

    msg = _loads_json(line)
    if not msg:
        log.warning("invalid JSON in executions bus (%s): %r", EXEC_BUS_PATH.name, line[:200])
        return 0

    published = 0
    for row in _iter_execution_rows(msg):
        trade_id = str(row.get("orderLinkId") or row.get("order_link_id") or row.get("orderId") or row.get("order_id") or "")
        ts_exec_raw = row.get("execTime") or row.get("exec_time") or row.get("T") or row.get("ts")
        ts_exec_ms = _normalize_ts_ms(ts_exec_raw)
        px = str(row.get("execPrice") or row.get("price") or "")
        qty = str(row.get("execQty") or row.get("qty") or "")
        side = str(row.get("side") or "")
        fill_key = f"live|{trade_id}|{ts_exec_ms}|{px}|{qty}|{side}"

        if not _dedupe_id(fill_key):
            continue

        exec_id = _exec_id_from_row(row)
        if exec_id and not _dedupe_id(f"execid|{exec_id}"):
            continue

        evt = _build_outcome_from_exec_row(row)
        if not evt:
            continue
        try:
            publish_ai_event(evt)
            published += 1
        except Exception as e:
            alert_bot_error("trade_outcome_recorder", f"publish_ai_event failed for {row.get('symbol')}: {e}", "ERROR")

    return published


# ---------------------------------------------------------------------------
# PAPER processing
# ---------------------------------------------------------------------------

def _process_paper_line(raw: bytes) -> int:
    try:
        line = raw.decode("utf-8", errors="replace")
    except Exception as e:
        log.warning("failed to decode paper stream line: %r", e)
        return 0

    line = _strip_bom(line).strip()
    if not line:
        return 0

    obj = _loads_json(line)
    if not obj:
        return 0

    evt = _build_outcome_from_paper_obj(obj)
    if not evt:
        return 0

    # Best-effort stable dedupe
    trade_id = str(evt.get("trade_id") or evt.get("extra", {}).get("trade_id") or "")
    sym = str(evt.get("symbol") or "")
    ts = str(evt.get("extra", {}).get("ts_exec_ms") or evt.get("ts") or evt.get("timestamp") or "")
    final_status = str(evt.get("extra", {}).get("final_status") or evt.get("final_status") or "")
    key = f"paper|{trade_id}|{sym}|{ts}|{final_status}"
    if not _dedupe_id(key):
        return 0

    try:
        publish_ai_event(evt)
        return 1
    except Exception as e:
        alert_bot_error("trade_outcome_recorder", f"publish_ai_event (paper) failed for {sym}: {e}", "ERROR")
        return 0


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def loop() -> None:
    poll_seconds = float(os.getenv("POLL_SECONDS", "0.25"))
    cursor_flush_every = int(os.getenv("CURSOR_FLUSH_EVERY", "50"))
    timeout_minutes = int(os.getenv("OUTCOME_TIMEOUT_MINUTES", "30"))
    reconcile_every = float(os.getenv("RECONCILE_EVERY_SECONDS", "10"))

    env_mode = _env_mode() or "AUTO"
    # If env says PAPER-like, use paper stream
    use_paper = _is_paper_like(env_mode) if env_mode != "AUTO" else False
    paper_path = _paper_stream_path() if use_paper else None

    source_name = "EXEC_BUS"
    source_path = EXEC_BUS_PATH

    if use_paper:
        if paper_path is None:
            log.warning("MODE=%s indicates PAPER-like, but no paper stream found. Set PAPER_LEDGER_PATH or ensure state has a paper*.jsonl file.", env_mode)
            # still fall back to exec bus to avoid hard crash
        else:
            source_name = "PAPER_STREAM"
            source_path = paper_path

    log.info(
        "Trade Outcome Recorder starting (account=%s, mode=%s, source=%s, source_path=%s, cursor=%s, ai_events_dir=%s, poll=%.2fs, flush_every=%d, timeout=%dm, reconcile=%.1fs)",
        ACCOUNT_LABEL, env_mode, source_name, source_path, CURSOR_PATH, AI_EVENTS_DIR, poll_seconds, cursor_flush_every, timeout_minutes, reconcile_every
    )

    pos = _load_cursor()
    log.info("Initial cursor position: %s", pos)

    since_flush = 0
    last_reconcile = 0.0

    while True:
        record_heartbeat("trade_outcome_recorder")

        now_s = time.time()
        if (now_s - last_reconcile) >= reconcile_every:
            try:
                n = _reconcile_pending_setups(timeout_minutes=timeout_minutes)
                if n > 0:
                    log.warning("Reconciler emitted %d synthetic terminal outcomes (idempotent; removed from pending).", n)
            except Exception as e:
                alert_bot_error("trade_outcome_recorder", f"reconcile error: {e}", "ERROR")
            last_reconcile = now_s

        try:
            # Re-evaluate paper stream periodically if in paper mode and file disappears/rotates
            if use_paper:
                p2 = _paper_stream_path()
                if p2 is not None:
                    source_path = p2

            if not source_path.exists():
                time.sleep(poll_seconds)
                continue

            size = source_path.stat().st_size
            if pos > size:
                log.info("stream truncated (size=%s < cursor=%s), resetting to 0", size, pos)
                pos = 0
                _save_cursor(pos)

            with source_path.open("rb") as f:
                f.seek(pos)
                for raw in f:
                    pos = f.tell()
                    if use_paper and source_name == "PAPER_STREAM":
                        _process_paper_line(raw)
                    else:
                        _process_live_line(raw)
                    since_flush += 1
                    if since_flush >= cursor_flush_every:
                        _save_cursor(pos)
                        since_flush = 0

            if since_flush > 0:
                _save_cursor(pos)
                since_flush = 0

            time.sleep(poll_seconds)

        except Exception as e:
            alert_bot_error("trade_outcome_recorder", f"loop error: {e}", "ERROR")
            time.sleep(1.0)


def main() -> None:
    try:
        loop()
    except KeyboardInterrupt:
        log.info("Trade Outcome Recorder stopped by user.")


if __name__ == "__main__":
    main()
