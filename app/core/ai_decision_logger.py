#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Flashback — AI Decision Logger (Phase 4)

Hard requirements:
- Never crash caller.
- Write exactly ONE JSON object per line (JSONL).
- Be safe under multi-process writers (best-effort Windows-friendly):
    • Use a lock file (msvcrt) to serialize read/dedupe/append.
    • Append bytes in one write call.
- Dedupe recent tail by a stable key so we don't spam duplicates.

Writes:
- state/ai_decisions.jsonl (default)
- Rotates to: ai_decisions.jsonl.1, .2, ... up to KEEP

Phase 4/5 hardening (coverage + determinism):
- Normalize / infer decision context fields when possible:
    • account_label, symbol, timeframe
- Provide "decision coverage guard" helpers:
    • decision_exists(...)
    • ensure_decision_exists(...)  <-- safe default BLOCK if missing

Optional strictness:
- AI_DECISIONS_REJECT_MISSING_CONTEXT=true/false (default false)
  If true, decisions that still lack account_label/symbol after inference are
  written to state/ai_decisions.rejected.jsonl (append-only) and NOT to canonical.
"""

from __future__ import annotations

import os
import time
import hashlib
from pathlib import Path
from typing import Any, Dict, Tuple, Optional

import orjson

DEFAULT_PATH = "state/ai_decisions.jsonl"
DEFAULT_REJECTED_PATH = "state/ai_decisions.rejected.jsonl"


# -------------------------
# env helpers
# -------------------------
def _env_int(name: str, default: str) -> int:
    try:
        return int(os.getenv(name, default).strip())
    except Exception:
        return int(default)


def _env_float(name: str, default: str) -> float:
    try:
        return float(os.getenv(name, default).strip())
    except Exception:
        return float(default)


def _env_bool(name: str, default: str = "false") -> bool:
    raw = os.getenv(name, default)
    return str(raw).strip().lower() in ("1", "true", "yes", "y", "on")


# -------------------------
# paths
# -------------------------
def _path() -> Path:
    p = os.getenv("AI_DECISIONS_PATH", DEFAULT_PATH).strip() or DEFAULT_PATH
    out = Path(p).resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    return out


def _rejected_path() -> Path:
    p = os.getenv("AI_DECISIONS_REJECTED_PATH", DEFAULT_REJECTED_PATH).strip() or DEFAULT_REJECTED_PATH
    out = Path(p).resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    return out


def _lock_path(base: Path) -> Path:
    lp = os.getenv("AI_DECISIONS_LOCK_PATH", "").strip()
    if lp:
        p = Path(lp).resolve()
        p.parent.mkdir(parents=True, exist_ok=True)
        return p
    return base.with_suffix(base.suffix + ".lock")


# -------------------------
# rotation
# -------------------------
def _rotate_file(path: Path, keep: int) -> None:
    """
    file -> file.1
    file.1 -> file.2
    ...
    delete file.keep
    """
    try:
        if keep <= 0 or not path.exists():
            return

        oldest = path.with_suffix(path.suffix + f".{keep}")
        try:
            if oldest.exists():
                oldest.unlink()
        except Exception:
            pass

        for i in range(keep - 1, 0, -1):
            src = path.with_suffix(path.suffix + f".{i}")
            dst = path.with_suffix(path.suffix + f".{i+1}")
            if src.exists():
                try:
                    src.replace(dst)
                except Exception:
                    pass

        dst1 = path.with_suffix(path.suffix + ".1")
        try:
            path.replace(dst1)
        except Exception:
            pass
    except Exception:
        return


# -------------------------
# utils
# -------------------------
def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_str(x: Any) -> str:
    try:
        if x is None:
            return ""
        return str(x).strip()
    except Exception:
        return ""


def _safe_upper(x: Any) -> str:
    return _safe_str(x).upper()


def _infer_account_label(d: Dict[str, Any]) -> str:
    # canonical
    acct = _safe_str(d.get("account_label"))
    if acct:
        return acct

    # alt fields seen in some modules
    acct = _safe_str(d.get("label"))
    if acct:
        return acct

    acct = _safe_str(d.get("account"))
    if acct:
        return acct

    # sometimes nested
    extra = d.get("extra")
    if isinstance(extra, dict):
        acct = _safe_str(extra.get("account_label"))
        if acct:
            return acct

    return ""


def _infer_symbol(d: Dict[str, Any]) -> str:
    sym = _safe_upper(d.get("symbol"))
    if sym:
        return sym

    sym = _safe_upper(d.get("sym"))
    if sym:
        return sym

    # sometimes nested
    extra = d.get("extra")
    if isinstance(extra, dict):
        sym = _safe_upper(extra.get("symbol"))
        if sym:
            return sym
        legacy = extra.get("legacy_action")
        if isinstance(legacy, dict):
            sym = _safe_upper(legacy.get("symbol"))
            if sym:
                return sym

    return ""


def _infer_timeframe(d: Dict[str, Any]) -> str:
    tf = _safe_str(d.get("timeframe"))
    if tf:
        return tf

    # some schemas use "tf"
    tf = _safe_str(d.get("tf"))
    if tf:
        return tf

    # sometimes nested setup/policy/extra
    extra = d.get("extra")
    if isinstance(extra, dict):
        tf = _safe_str(extra.get("timeframe"))
        if tf:
            return tf

    return ""


def _normalize_decision_context(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ensure account_label/symbol/timeframe are present when possible.
    Does not throw. Returns a new dict.
    """
    out = dict(payload)

    acct = _infer_account_label(out)
    sym = _infer_symbol(out)
    tf = _infer_timeframe(out)

    # apply if missing
    if not _safe_str(out.get("account_label")) and acct:
        out["account_label"] = acct
    if not _safe_str(out.get("symbol")) and sym:
        out["symbol"] = sym
    if not _safe_str(out.get("timeframe")) and tf:
        out["timeframe"] = tf

    # normalize symbol uppercase if present
    if _safe_str(out.get("symbol")):
        out["symbol"] = _safe_upper(out.get("symbol"))

    return out


def _dedupe_key(d: Dict[str, Any]) -> str:
    """
    Stable-ish key across both formats you currently have:
      - Pilot decision rows: schema_version=1 + 'decision'
      - Executor audit rows: event_type='ai_decision' + decision_code/allow/size_multiplier

    We DO NOT include timestamp in the key, because duplicates often differ only by ts.
    """
    trade_id = _safe_str(d.get("trade_id"))
    acct = _safe_str(d.get("account_label"))
    sym = _safe_upper(d.get("symbol"))
    tf = _safe_str(d.get("timeframe"))

    # Pilot-style
    if d.get("schema_version") == 1 and ("decision" in d):
        decision = _safe_str(d.get("decision"))
        gates = d.get("gates") or {}
        reason = ""
        if isinstance(gates, dict):
            reason = _safe_str(gates.get("reason"))
        return f"PILOT|{trade_id}|{acct}|{sym}|{tf}|{decision}|{reason}"

    # Executor-audit style
    if _safe_str(d.get("event_type")) == "ai_decision":
        decision_code = _safe_str(d.get("decision_code") or d.get("decision") or "")
        allow = _safe_str(d.get("allow") if "allow" in d else "")
        sm = _safe_str(d.get("size_multiplier") if "size_multiplier" in d else "")
        return f"EXEC|{trade_id}|{acct}|{sym}|{tf}|{decision_code}|{allow}|{sm}"

    # Unknown writer style: still dedupe by core identity + full payload hash
    core = f"UNK|{trade_id}|{acct}|{sym}|{tf}"
    h = hashlib.md5(orjson.dumps(d, option=orjson.OPT_SORT_KEYS, default=str)).hexdigest()
    return core + "|" + h


def _tail_recent_keys(path: Path, tail_lines: int) -> Tuple[set, int]:
    """
    Read last N JSONL objects and return:
      - set of dedupe keys
      - count of bad JSON lines encountered in tail
    """
    keys = set()
    bad = 0
    if tail_lines <= 0 or not path.exists():
        return keys, bad

    try:
        data = path.read_bytes()
        if not data:
            return keys, bad
        lines = data.splitlines()[-tail_lines:]
        for b in lines:
            s = b.strip()
            if not s or not s.startswith(b"{"):
                continue
            try:
                d = orjson.loads(s)
                if isinstance(d, dict):
                    keys.add(_dedupe_key(d))
            except Exception:
                bad += 1
    except Exception:
        return keys, bad

    return keys, bad


def _append_bytes_atomic(path: Path, line_bytes: bytes) -> None:
    """
    Append bytes in ONE write call. Best-effort atomic append.
    """
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("ab") as f:
            f.write(line_bytes)
    except Exception:
        return


class _FileLock:
    """
    Tiny cross-process lock via a lock file.
    Uses msvcrt on Windows; falls back to no-op elsewhere.
    """
    def __init__(self, lock_path: Path, timeout_sec: float = 2.5) -> None:
        self.lock_path = lock_path
        self.timeout_sec = timeout_sec
        self._fh = None

    def __enter__(self):
        try:
            self.lock_path.parent.mkdir(parents=True, exist_ok=True)
            self._fh = open(self.lock_path, "a+b")
            try:
                import msvcrt  # Windows only
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


# -------------------------
# decision existence + coverage guard
# -------------------------
def decision_exists(*, trade_id: str, account_label: str = "", symbol: str = "", tail_lines: Optional[int] = None) -> bool:
    """
    Best-effort existence check using a tail scan (fast, no full-file read).
    If account_label/symbol provided, it checks those too.
    """
    try:
        tid = _safe_str(trade_id)
        if not tid:
            return False

        path = _path()
        if not path.exists():
            return False

        tail = int(tail_lines) if tail_lines is not None else _env_int("AI_DECISIONS_EXISTS_TAIL", "2000")
        if tail <= 0:
            tail = 2000

        acct = _safe_str(account_label)
        sym = _safe_upper(symbol)

        data = path.read_bytes()
        if not data:
            return False

        lines = data.splitlines()[-tail:]
        for b in reversed(lines):
            s = b.strip()
            if not s or not s.startswith(b"{"):
                continue
            try:
                d = orjson.loads(s)
            except Exception:
                continue
            if not isinstance(d, dict):
                continue
            if _safe_str(d.get("trade_id")) != tid:
                continue

            # if filters provided, enforce them
            if acct and _safe_str(d.get("account_label")) != acct:
                continue
            if sym and _safe_upper(d.get("symbol")) != sym:
                continue

            return True

        return False
    except Exception:
        return False


def ensure_decision_exists(
    *,
    trade_id: str,
    account_label: str,
    symbol: str,
    timeframe: str = "",
    reason: str = "decision_coverage_guard",
    mode: str = "COVERAGE",
    allow: bool = False,
    size_multiplier: float = 1.0,
) -> None:
    """
    Coverage guard:
    - If a decision for (trade_id, account_label, symbol) does not exist, append one.
    - Default is BLOCK (allow=False). Safer than silently letting trades/outcomes float.
    """
    try:
        tid = _safe_str(trade_id)
        acct = _safe_str(account_label)
        sym = _safe_upper(symbol)
        tf = _safe_str(timeframe)

        if not tid or not acct or not sym:
            # Can't meaningfully cover. Do nothing. Caller bug.
            return

        if decision_exists(trade_id=tid, account_label=acct, symbol=sym):
            return

        payload: Dict[str, Any] = {
            "schema_version": 1,
            "ts": _now_ms(),
            "trade_id": tid,
            "decision": "ALLOW_COVERAGE" if allow else "BLOCKED_BY_GATES",
            "tier_used": "NONE" if not allow else "COVERAGE",
            "memory": None,
            "gates": {"reason": reason},
            "proposed_action": None,
            "size_multiplier": float(size_multiplier),
            "allow": bool(allow),
            "reason": reason,
            "mode": mode,
            "account_label": acct,
            "symbol": sym,
        }
        if tf:
            payload["timeframe"] = tf

        append_decision(payload)
    except Exception:
        return


# -------------------------
# main writer
# -------------------------
def append_decision(decision: Dict[str, Any]) -> None:
    """
    Append one decision as JSONL. Never throws.
    - Rotates on cap.
    - Locks + tail-dedupes to prevent duplicate spam and corruption.
    - Normalizes context fields to reduce ambiguous joins.
    """
    try:
        path = _path()

        warn_mb = _env_float("AI_DECISIONS_WARN_MB", "10")
        cap_mb = _env_float("AI_DECISIONS_CAP_MB", "50")
        keep = _env_int("AI_DECISIONS_KEEP", "3")
        tail = _env_int("AI_DECISIONS_DEDUPE_TAIL", "250")
        lock_timeout = _env_float("AI_DECISIONS_LOCK_TIMEOUT_SEC", "2.5")

        reject_missing_context = _env_bool("AI_DECISIONS_REJECT_MISSING_CONTEXT", "false")
        rejected_path = _rejected_path()

        payload = _normalize_decision_context(dict(decision))

        # Ensure a timestamp exists (useful for audit)
        if "ts_ms" not in payload and "ts" not in payload:
            payload["ts_ms"] = _now_ms()

        # If still missing context, optionally reject into a separate file
        acct = _safe_str(payload.get("account_label"))
        sym = _safe_upper(payload.get("symbol"))
        if reject_missing_context and (not acct or not sym):
            try:
                payload.setdefault("extra", {})
                if isinstance(payload["extra"], dict):
                    payload["extra"]["reject_reason"] = "missing_context_after_infer"
                line_rej = orjson.dumps(payload, option=orjson.OPT_SORT_KEYS, default=str) + b"\n"
                _append_bytes_atomic(rejected_path, line_rej)
            except Exception:
                pass
            return

        line = orjson.dumps(payload, option=orjson.OPT_SORT_KEYS, default=str) + b"\n"
        key = _dedupe_key(payload)

        lockp = _lock_path(path)

        with _FileLock(lockp, timeout_sec=lock_timeout):
            # Rotate if over cap
            try:
                if path.exists():
                    size_mb = path.stat().st_size / (1024 * 1024)
                    if size_mb >= cap_mb:
                        _rotate_file(path, keep=keep)
            except Exception:
                pass

            # Dedupe against recent tail
            try:
                recent_keys, _bad_tail = _tail_recent_keys(path, tail_lines=tail)
                if key in recent_keys:
                    return
            except Exception:
                pass

            _append_bytes_atomic(path, line)

            # warn only (no logging dependency here)
            try:
                if path.exists():
                    size_mb = path.stat().st_size / (1024 * 1024)
                    if size_mb >= warn_mb:
                        pass
            except Exception:
                pass

    except Exception:
        return
