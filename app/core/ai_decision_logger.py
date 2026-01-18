#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import time
import json
import hashlib
from pathlib import Path
from typing import Any, Dict, Tuple, Optional, List

import orjson

"""
Flashback — AI Decision Logger (Phase 4)

Hard requirements:
- Never crash caller.
- Write exactly ONE JSON object per line (JSONL).
- Best-effort multi-process safety on Windows:
    • Use a lock file (msvcrt) to serialize rotate/dedupe/append.
    • Append bytes in one write call.
- Dedupe recent tail by stable key to suppress duplicates.

Writes:
- state/ai_decisions.jsonl (default)
- Rotates to: ai_decisions.jsonl.1, .2, ... up to KEEP

Phase 4/5 hardening:
- Normalize / infer decision context fields:
    • account_label, symbol, timeframe
- Decision coverage helpers:
    • decision_exists(...)
    • ensure_decision_exists(...)  <-- safe default BLOCK if missing

Optional strictness:
- AI_DECISIONS_REJECT_MISSING_CONTEXT=true/false (default false)
  If true, decisions still lacking account_label/symbol after inference are
  written to state/ai_decisions.rejected.jsonl and NOT to canonical.

Determinism upgrades:
- Canonical dedupe key (stage-aware):
    (trade_id, stage, account_label, symbol, timeframe)

IMPORTANT FIX (2025-12-19 -> hardened further 2025-12-19b -> v2.9.6 alignment):
- Pilot rows MUST be tagged with event_type="pilot_decision".
- Pilot dedupe: ONE per (trade_id, account_label, symbol, timeframe).
- ai_decision rows missing BOTH decision_code and decision are rejected/dropped.
- ts_ms is stamped if missing OR None OR invalid.
- timeframe is ALWAYS normalized (e.g., "5" -> "5m") to prevent Phase 6 bucket mismatches.
- meta.source/meta.stage always present for pilot_decision rows.

PHASE 7 ADDITION (2025-12-21):
- Stamp deterministic AI State Snapshot linkage onto every decision:
    • snapshot_fp (sha256 over canonicalized snapshot, with volatile fields scrubbed)
    • snapshot_schema_version
    • snapshot_mode ("DRY_RUN" / "LIVE")
- Optional: persist snapshots to state/ai_snapshots.jsonl (dedupe by snapshot_fp)
  Controlled by env:
    • AI_DECISIONS_STAMP_SNAPSHOT=true/false (default true)
    • AI_SNAPSHOTS_ENABLE=true/false (default false)

AUDIT (optional):
- If AI_DECISION_AUDIT_ENABLE=true, emit minimal audit rows to:
    state/audit/decision_audit.v1.jsonl
- Audit append is lock-protected and atomic.
"""


# -------------------------
# spine_api imports (optional)
# -------------------------
try:
    from app.core.spine_api import AI_DECISIONS_PATH as _AI_DECISIONS_PATH
    from app.core.spine_api import AI_SNAPSHOTS_PATH as _AI_SNAPSHOTS_PATH
    from app.core.spine_api import now_ms as _spine_now_ms
except Exception:  # pragma: no cover
    _AI_DECISIONS_PATH = None  # type: ignore
    _AI_SNAPSHOTS_PATH = None  # type: ignore
    _spine_now_ms = None  # type: ignore


def _now_ms() -> int:
    """Always-safe ms timestamp. Never throws."""
    try:
        if callable(_spine_now_ms):  # type: ignore[arg-type]
            v = _spine_now_ms()  # type: ignore[misc]
            if isinstance(v, int) and v > 0:
                return v
    except Exception:
        pass
    return int(time.time() * 1000)


DEFAULT_PATH = str(_AI_DECISIONS_PATH) if _AI_DECISIONS_PATH is not None else "state/ai_decisions.jsonl"
DEFAULT_REJECTED_PATH = "state/ai_decisions.rejected.jsonl"

DEFAULT_SNAPSHOTS_PATH = str(_AI_SNAPSHOTS_PATH) if _AI_SNAPSHOTS_PATH is not None else "state/ai_snapshots.jsonl"
DEFAULT_SNAPSHOTS_LOCK_SUFFIX = ".lock"

# -------------------------
# optional audit
# -------------------------
ROOT = Path(__file__).resolve().parents[2]
AUDIT_DIR = ROOT / "state" / "audit"
AUDIT_DIR.mkdir(parents=True, exist_ok=True)
AUDIT_PATH = AUDIT_DIR / "decision_audit.v1.jsonl"
AUDIT_LOCK = AUDIT_PATH.with_suffix(AUDIT_PATH.suffix + ".lock")


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


def _emit_decision_audit(decision: Dict[str, Any]) -> None:
    """
    Optional, minimal audit record.
    Must never crash caller.
    """
    try:
        if not _env_bool("AI_DECISION_AUDIT_ENABLE", "false"):
            return

        payload = {
            "ts_ms": _now_ms(),
            "decision_code": decision.get("decision_code") or decision.get("decision"),
            "allow": decision.get("allow"),
            "confidence": decision.get("confidence"),
            "expectancy_adj": decision.get("expectancy_adj"),
            "n": decision.get("n"),
            "scoreboard_version": decision.get("scoreboard_version"),
        }
        line = orjson.dumps(payload, option=orjson.OPT_SORT_KEYS, default=str) + b"\n"

        with _FileLock(AUDIT_LOCK, timeout_sec=_env_float("AI_DECISION_AUDIT_LOCK_TIMEOUT_SEC", "2.0")):
            _append_bytes_atomic(AUDIT_PATH, line)
    except Exception:
        return


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


def _snapshots_path() -> Path:
    p = os.getenv("AI_SNAPSHOTS_PATH", DEFAULT_SNAPSHOTS_PATH).strip() or DEFAULT_SNAPSHOTS_PATH
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


def _snapshots_lock_path(base: Path) -> Path:
    lp = os.getenv("AI_SNAPSHOTS_LOCK_PATH", "").strip()
    if lp:
        p = Path(lp).resolve()
        p.parent.mkdir(parents=True, exist_ok=True)
        return p
    return base.with_suffix(base.suffix + DEFAULT_SNAPSHOTS_LOCK_SUFFIX)


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
def _safe_str(x: Any) -> str:
    try:
        if x is None:
            return ""
        return str(x).strip()
    except Exception:
        return ""


def _safe_upper(x: Any) -> str:
    return _safe_str(x).upper()


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        return int(x)
    except Exception:
        return default


def _normalize_timeframe(tf: Any, default: str = "5m") -> str:
    """
    Canonical timeframe normalization:
    - "5" -> "5m"
    - "5m" -> "5m"
    - "1h" -> "1h"
    - ""/None -> default
    Never throws.
    """
    s = ""
    try:
        s = str(tf).strip().lower()
    except Exception:
        s = ""
    if not s:
        return default
    if s.endswith(("m", "h", "d", "w")):
        return s
    if s.isdigit():
        return f"{s}m"
    return s or default


def _infer_account_label(d: Dict[str, Any]) -> str:
    acct = _safe_str(d.get("account_label")) or _safe_str(d.get("label")) or _safe_str(d.get("account"))
    if acct:
        return acct
    extra = d.get("extra")
    if isinstance(extra, dict):
        acct = _safe_str(extra.get("account_label"))
        if acct:
            return acct
    # last-resort fallback: env (useful in isolated per-sub writers)
    env_acct = _safe_str(os.getenv("ACCOUNT_LABEL", ""))
    return env_acct


def _infer_symbol(d: Dict[str, Any]) -> str:
    sym = _safe_upper(d.get("symbol")) or _safe_upper(d.get("sym"))
    if sym:
        return sym
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
    tf = _safe_str(d.get("timeframe")) or _safe_str(d.get("tf"))
    if tf:
        return tf
    extra = d.get("extra")
    if isinstance(extra, dict):
        tf = _safe_str(extra.get("timeframe"))
        if tf:
            return tf
    return ""


def _normalize_decision_context(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    v2.9.6 alignment:
    - Always stamp canonical account_label/symbol/timeframe (best-effort infer).
    - Always normalize symbol to UPPER.
    - Always normalize timeframe to canonical ("5" -> "5m").
    """
    out = dict(payload)

    acct = _infer_account_label(out)
    sym = _infer_symbol(out)
    tf_raw = _infer_timeframe(out)

    # Only infer if missing
    if not _safe_str(out.get("account_label")) and acct:
        out["account_label"] = acct
    if not _safe_str(out.get("symbol")) and sym:
        out["symbol"] = sym
    if not _safe_str(out.get("timeframe")) and tf_raw:
        out["timeframe"] = tf_raw

    # Canonicalize always
    if _safe_str(out.get("symbol")):
        out["symbol"] = _safe_upper(out.get("symbol"))

    tf = _safe_str(out.get("timeframe"))
    out["timeframe"] = _normalize_timeframe(tf, default=_safe_str(os.getenv("AI_DEFAULT_TIMEFRAME", "5m")) or "5m")

    return out


def _infer_stage(d: Dict[str, Any]) -> str:
    try:
        extra = d.get("extra")
        if isinstance(extra, dict):
            st = _safe_str(extra.get("stage"))
            if st:
                return st
        meta = d.get("meta")
        if isinstance(meta, dict):
            st = _safe_str(meta.get("stage"))
            if st:
                return st
        if _safe_str(d.get("event_type")) == "ai_decision":
            return "pre_entry"
        gates = d.get("gates")
        if isinstance(gates, dict) and _safe_str(gates.get("enforced")):
            return "post_enforce"
        return "unknown"
    except Exception:
        return "unknown"


def _is_pilot_row(d: Dict[str, Any]) -> bool:
    """
    Pilot rows are schema_version=1 and have 'decision' field (Phase 4 pilot decisions).
    Also treat explicit event_type=pilot_decision as pilot.
    """
    try:
        if _safe_str(d.get("event_type")) == "pilot_decision":
            return True
        return d.get("schema_version") == 1 and ("decision" in d)
    except Exception:
        return False


def _is_legacy_pilot_spam(d: Dict[str, Any]) -> bool:
    try:
        if not _is_pilot_row(d):
            return False
        if _safe_str(d.get("event_type")):
            return False
        meta = d.get("meta")
        if isinstance(meta, dict) and _safe_str(meta.get("source")):
            return False
        return True
    except Exception:
        return False


def _pilot_dedupe_key(d: Dict[str, Any]) -> str:
    tid = _safe_str(d.get("trade_id"))
    acct = _safe_str(d.get("account_label"))
    sym = _safe_upper(d.get("symbol"))
    tf = _normalize_timeframe(d.get("timeframe"), default="5m")
    return f"PILOT_CANON|{tid}|{acct}|{sym}|{tf}"


def _canonical_dedupe_key(d: Dict[str, Any]) -> str:
    if _is_pilot_row(d) or _safe_str(d.get("event_type")) == "pilot_decision":
        return _pilot_dedupe_key(d)

    trade_id = _safe_str(d.get("trade_id"))
    acct = _safe_str(d.get("account_label"))
    sym = _safe_upper(d.get("symbol"))
    tf = _normalize_timeframe(d.get("timeframe"), default="5m")
    stage = _infer_stage(d)
    return f"CANON|{trade_id}|{stage}|{acct}|{sym}|{tf}"


def _dedupe_key(d: Dict[str, Any]) -> str:
    trade_id = _safe_str(d.get("trade_id"))
    acct = _safe_str(d.get("account_label"))
    sym = _safe_upper(d.get("symbol"))
    tf = _normalize_timeframe(d.get("timeframe"), default="5m")

    if _is_pilot_row(d) or _safe_str(d.get("event_type")) == "pilot_decision":
        decision = _safe_str(d.get("decision"))
        gates = d.get("gates") or {}
        reason = ""
        if isinstance(gates, dict):
            reason = _safe_str(gates.get("reason"))
        return f"PILOT|{trade_id}|{acct}|{sym}|{tf}|{decision}|{reason}"

    if _safe_str(d.get("event_type")) == "ai_decision":
        decision_code = _safe_str(d.get("decision_code") or d.get("decision") or "")
        allow = _safe_str(d.get("allow") if "allow" in d else "")
        sm = _safe_str(d.get("size_multiplier") if "size_multiplier" in d else "")
        return f"EXEC|{trade_id}|{acct}|{sym}|{tf}|{decision_code}|{allow}|{sm}"

    core = f"UNK|{trade_id}|{acct}|{sym}|{tf}"
    try:
        h = hashlib.md5(orjson.dumps(d, option=orjson.OPT_SORT_KEYS, default=str)).hexdigest()
    except Exception:
        h = "0"
    return core + "|" + h


def _tail_recent_keys(path: Path, tail_lines: int) -> Tuple[set, int]:
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
                    # Normalize timeframe before keying to prevent drift-based dupes
                    d2 = _normalize_decision_context(d)
                    keys.add(_canonical_dedupe_key(d2))
                    keys.add(_dedupe_key(d2))
            except Exception:
                bad += 1
    except Exception:
        return keys, bad

    return keys, bad


def _append_bytes_atomic(path: Path, line_bytes: bytes) -> None:
    """
    Atomic append-bytes best-effort:
    - one os.write() call per line
    - avoids Path.open('ab') signature
    """
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(path), os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o666)
        try:
            os.write(fd, line_bytes)
        finally:
            os.close(fd)
    except Exception:
        return


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


# -------------------------
# Phase 7: snapshot linkage
# -------------------------
_VOLATILE_DROP_KEYS = {
    "ts", "ts_ms", "timestamp", "time_ms", "time",
    "snapshot_age_sec", "snapshot_age_ms", "age_sec", "age_ms",
    "updated_ms", "orderbook_updated_ms", "trades_updated_ms",
}


def _scrub_for_fp(x: Any) -> Any:
    """
    Scrub volatile fields so snapshot_fp reflects meaningful state, not clock noise.
    Conservative: drop explicit volatile keys; do NOT blindly drop all *_ms keys.
    Never throws.
    """
    try:
        if isinstance(x, dict):
            out: Dict[str, Any] = {}
            for k, v in x.items():
                ks = str(k)
                if ks in _VOLATILE_DROP_KEYS:
                    continue

                # positions.raw is huge/noisy; rely on by_symbol map instead
                if ks == "raw" and isinstance(v, list):
                    continue

                out[ks] = _scrub_for_fp(v)
            return out
        if isinstance(x, list):
            return [_scrub_for_fp(i) for i in x]
        return x
    except Exception:
        return x


def _canon_json(obj: Any) -> str:
    try:
        return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)
    except Exception:
        try:
            return json.dumps(str(obj), sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        except Exception:
            return str(obj)


def _snapshot_fp(snapshot: Dict[str, Any]) -> str:
    try:
        scrubbed = _scrub_for_fp(snapshot)
        s = _canon_json(scrubbed).encode("utf-8", errors="ignore")
        return hashlib.sha256(s).hexdigest()
    except Exception:
        return ""


def _snapshot_mode() -> str:
    raw = os.getenv("EXEC_DRY_RUN", "false").strip().lower()
    return "DRY_RUN" if raw in ("1", "true", "yes", "y", "on") else "LIVE"


def _build_snapshot_for_payload(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        from app.core import ai_state_bus  # local import to avoid import-time coupling

        sym = _safe_upper(payload.get("symbol"))
        focus: Optional[List[str]] = [sym] if sym else None

        include_trades = _env_bool("AI_SNAPSHOT_INCLUDE_TRADES", "false")
        trades_limit = _env_int("AI_SNAPSHOT_TRADES_LIMIT", "50")
        include_orderbook = _env_bool("AI_SNAPSHOT_INCLUDE_ORDERBOOK", "true")

        snap = ai_state_bus.build_ai_snapshot(
            focus_symbols=focus,
            include_trades=include_trades,
            trades_limit=trades_limit,
            include_orderbook=include_orderbook,
        )
        return snap if isinstance(snap, dict) else None
    except Exception:
        return None


def _tail_recent_snapshot_fps(path: Path, tail_lines: int) -> set:
    fps = set()
    if tail_lines <= 0 or not path.exists():
        return fps
    try:
        data = path.read_bytes()
        if not data:
            return fps
        lines = data.splitlines()[-tail_lines:]
        for b in lines:
            s = b.strip()
            if not s or not s.startswith(b"{"):
                continue
            try:
                d = orjson.loads(s)
            except Exception:
                continue
            if isinstance(d, dict):
                fp = _safe_str(d.get("snapshot_fp"))
                if fp:
                    fps.add(fp)
    except Exception:
        return fps
    return fps


def _maybe_persist_snapshot(fp: str, snapshot: Dict[str, Any]) -> None:
    try:
        if not fp:
            return
        if not _env_bool("AI_SNAPSHOTS_ENABLE", "false"):
            return

        spath = _snapshots_path()
        lockp = _snapshots_lock_path(spath)
        tail = _env_int("AI_SNAPSHOTS_DEDUPE_TAIL", "2500")
        cap_mb = _env_float("AI_SNAPSHOTS_CAP_MB", "200")
        keep = _env_int("AI_SNAPSHOTS_KEEP", "2")

        row = {
            "ts_ms": _now_ms(),
            "snapshot_fp": fp,
            "snapshot_schema_version": snapshot.get("schema_version"),
            "snapshot_mode": _snapshot_mode(),
            "snapshot": snapshot,
        }
        line = orjson.dumps(row, option=orjson.OPT_SORT_KEYS, default=str) + b"\n"

        with _FileLock(lockp, timeout_sec=_env_float("AI_SNAPSHOTS_LOCK_TIMEOUT_SEC", "2.5")):
            try:
                if spath.exists():
                    size_mb = spath.stat().st_size / (1024 * 1024)
                    if size_mb >= cap_mb:
                        _rotate_file(spath, keep=keep)
            except Exception:
                pass

            try:
                recent = _tail_recent_snapshot_fps(spath, tail_lines=tail)
                if fp in recent:
                    return
            except Exception:
                pass

            _append_bytes_atomic(spath, line)
    except Exception:
        return


def _stamp_snapshot_linkage(payload: Dict[str, Any]) -> None:
    try:
        if not _env_bool("AI_DECISIONS_STAMP_SNAPSHOT", "true"):
            return

        if _safe_str(payload.get("snapshot_fp")):
            return

        snap = _build_snapshot_for_payload(payload)
        if not snap:
            payload["snapshot_mode"] = _snapshot_mode()
            return

        fp = _snapshot_fp(snap)
        if fp:
            payload["snapshot_fp"] = fp
            payload["snapshot_schema_version"] = snap.get("schema_version")
            payload["snapshot_mode"] = _snapshot_mode()
            _maybe_persist_snapshot(fp, snap)
        else:
            payload["snapshot_mode"] = _snapshot_mode()
    except Exception:
        return


# -------------------------
# decision existence + coverage guard
# -------------------------
def decision_exists(*, trade_id: str, account_label: str = "", symbol: str = "", tail_lines: Optional[int] = None) -> bool:
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
    stage: str = "coverage_guard",
) -> None:
    try:
        tid = _safe_str(trade_id)
        acct = _safe_str(account_label)
        sym = _safe_upper(symbol)
        tf = _normalize_timeframe(timeframe, default="5m")

        if not tid or not acct or not sym:
            return

        if decision_exists(trade_id=tid, account_label=acct, symbol=sym):
            return

        payload: Dict[str, Any] = {
            "schema_version": 1,
            "ts_ms": _now_ms(),
            "ts": _now_ms(),
            "trade_id": tid,
            "decision": "ALLOW_COVERAGE" if allow else "BLOCKED_BY_GATES",
            "tier_used": "COVERAGE" if allow else "NONE",
            "memory": None,
            "gates": {"reason": reason},
            "proposed_action": None,
            "size_multiplier": float(size_multiplier),
            "allow": bool(allow),
            "reason": reason,
            "mode": mode,
            "account_label": acct,
            "symbol": sym,
            "timeframe": tf,
            "meta": {"source": "coverage_guard", "stage": stage},
            "event_type": "pilot_decision",
            "extra": {"stage": stage},
        }

        append_decision(payload)
    except Exception:
        return


# -------------------------
# main writer
# -------------------------
def append_decision(decision: Dict[str, Any]) -> None:
    try:
        path = _path()

        cap_mb = _env_float("AI_DECISIONS_CAP_MB", "50")
        keep = _env_int("AI_DECISIONS_KEEP", "3")
        tail = _env_int("AI_DECISIONS_DEDUPE_TAIL", "250")
        lock_timeout = _env_float("AI_DECISIONS_LOCK_TIMEOUT_SEC", "2.5")

        reject_missing_context = _env_bool("AI_DECISIONS_REJECT_MISSING_CONTEXT", "false")
        allow_legacy_pilot = _env_bool("AI_DECISIONS_ALLOW_LEGACY_PILOT", "false")
        rejected_path = _rejected_path()

        payload = _normalize_decision_context(dict(decision))

        # Phase 7: stamp snapshot linkage early (before dedupe)
        _stamp_snapshot_linkage(payload)

        # Normalize / infer event_type if missing
        et = _safe_str(payload.get("event_type"))
        if not et:
            et = "ai_decision"
            payload["event_type"] = et

        # Ensure schema_version
        sv = _safe_int(payload.get("schema_version"), default=0)
        et = _safe_str(payload.get("event_type"))
        if sv <= 0:
            payload["schema_version"] = 1 if et == "pilot_decision" else 2

        # Pilot tagging: normalize legacy pilot input rows (schema_version==1 + decision => pilot_decision)
        try:
            if payload.get("schema_version") == 1 and ("decision" in payload) and (_safe_str(payload.get("event_type")) != "pilot_decision"):
                payload["event_type"] = "pilot_decision"
        except Exception:
            pass

        # Ensure pilot meta.source/meta.stage always present (v2.9.6 alignment)
        try:
            if _safe_str(payload.get("event_type")) == "pilot_decision":
                payload.setdefault("meta", {})
                if isinstance(payload["meta"], dict):
                    payload["meta"].setdefault("source", "ai_pilot")
                    payload["meta"].setdefault("stage", "pilot")
        except Exception:
            pass

        # Normalize decision_code from decision/payload
        dc = _safe_str(payload.get("decision_code"))
        d = _safe_str(payload.get("decision"))
        pl = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
        if not dc and isinstance(pl, dict):
            dc = _safe_str(pl.get("decision_code"))
        if not d and isinstance(pl, dict):
            d = _safe_str(pl.get("decision"))

        if not dc and d:
            payload["decision_code"] = d
            dc = d

        if _safe_str(dc).upper() in ("NO_DECISION",):
            return

        # Force stage tag for stable dedupe behavior
        try:
            payload.setdefault("extra", {})
            if isinstance(payload["extra"], dict):
                stage = _safe_str(payload["extra"].get("stage"))
                if not stage:
                    payload["extra"]["stage"] = "pilot" if _safe_str(payload.get("event_type")) == "pilot_decision" else "enforced"
        except Exception:
            pass

        # ts_ms stamping + legacy ts backfill
        try:
            ts_ms_i = _safe_int(payload.get("ts_ms"), default=0)
            ts_i = _safe_int(payload.get("ts"), default=0)
            if ts_ms_i <= 0 and ts_i <= 0:
                payload["ts_ms"] = _now_ms()
                payload["ts"] = payload["ts_ms"]
            elif ts_ms_i <= 0 and ts_i > 0:
                payload["ts_ms"] = ts_i
                payload["ts"] = ts_i
            else:
                payload["ts_ms"] = ts_ms_i
                if ts_i <= 0:
                    payload["ts"] = ts_ms_i
        except Exception:
            payload["ts_ms"] = _now_ms()
            payload["ts"] = payload["ts_ms"]

        # Drop legacy spam pilot rows unless explicitly allowed
        if _is_legacy_pilot_spam(payload) and not allow_legacy_pilot:
            return

        # Reject junk ai_decision rows missing both decision_code and decision
        et = _safe_str(payload.get("event_type"))
        if et == "ai_decision":
            dc2 = _safe_str(payload.get("decision_code"))
            d2 = _safe_str(payload.get("decision"))
            if not dc2 and not d2:
                try:
                    payload.setdefault("extra", {})
                    if isinstance(payload["extra"], dict):
                        payload["extra"]["reject_reason"] = "ai_decision_missing_decision_code_and_decision"
                    line_rej = orjson.dumps(payload, option=orjson.OPT_SORT_KEYS, default=str) + b"\n"
                    _append_bytes_atomic(rejected_path, line_rej)
                except Exception:
                    pass
                return
            if not dc2 and d2:
                payload["decision_code"] = d2

        # Enforce canonical symbol/timeframe again (safety net)
        try:
            if _safe_str(payload.get("symbol")):
                payload["symbol"] = _safe_upper(payload.get("symbol"))
            payload["timeframe"] = _normalize_timeframe(payload.get("timeframe"), default=_safe_str(os.getenv("AI_DEFAULT_TIMEFRAME", "5m")) or "5m")
        except Exception:
            pass

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

        # Emit optional audit (never blocks)
        _emit_decision_audit(payload)

        line = orjson.dumps(payload, option=orjson.OPT_SORT_KEYS, default=str) + b"\n"

        canon_key = _canonical_dedupe_key(payload)
        legacy_key = _dedupe_key(payload)

        lockp = _lock_path(path)

        with _FileLock(lockp, timeout_sec=lock_timeout):
            try:
                if path.exists():
                    size_mb = path.stat().st_size / (1024 * 1024)
                    if size_mb >= cap_mb:
                        _rotate_file(path, keep=keep)
            except Exception:
                pass

            try:
                recent_keys, _bad_tail = _tail_recent_keys(path, tail_lines=tail)
                if canon_key in recent_keys or legacy_key in recent_keys:
                    return
            except Exception:
                pass

            _append_bytes_atomic(path, line)

    except Exception:
        return
