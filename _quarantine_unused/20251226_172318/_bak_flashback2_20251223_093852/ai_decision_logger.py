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

Determinism upgrades (duplicate suppression across formats):
- Canonical dedupe key is stage-aware:
    (trade_id, stage, account_label, symbol, timeframe)

IMPORTANT FIX (2025-12-19 -> hardened further 2025-12-19b):
- Pilot rows MUST be tagged with event_type="pilot_decision".
- Pilot dedupe is ONE per (trade_id, account_label, symbol, timeframe) regardless of reason/memory_fp.
- ai_decision rows missing BOTH decision_code and decision are rejected/dropped.
- ts_ms is stamped if missing OR None OR invalid.

PHASE 7 ADDITION (2025-12-21):
- Stamp deterministic AI State Snapshot linkage onto every decision:
    • snapshot_fp (sha256 over canonicalized snapshot, with volatile fields scrubbed)
    • snapshot_schema_version
    • snapshot_mode ("DRY_RUN" / "LIVE")
- Optional: persist snapshots to state/ai_snapshots.jsonl (dedupe by snapshot_fp)
  Controlled by env:
    • AI_DECISIONS_STAMP_SNAPSHOT=true/false (default true)
    • AI_SNAPSHOTS_ENABLE=true/false (default false)
"""

from __future__ import annotations

import os
import time
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, Tuple, Optional, List

import orjson

DEFAULT_PATH = "state/ai_decisions.jsonl"
DEFAULT_REJECTED_PATH = "state/ai_decisions.rejected.jsonl"

DEFAULT_SNAPSHOTS_PATH = "state/ai_snapshots.jsonl"
DEFAULT_SNAPSHOTS_LOCK_SUFFIX = ".lock"


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
    # default: sibling lock
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


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        return int(x)
    except Exception:
        return default


def _infer_account_label(d: Dict[str, Any]) -> str:
    acct = _safe_str(d.get("account_label"))
    if acct:
        return acct
    acct = _safe_str(d.get("label"))
    if acct:
        return acct
    acct = _safe_str(d.get("account"))
    if acct:
        return acct
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
    tf = _safe_str(d.get("tf"))
    if tf:
        return tf
    extra = d.get("extra")
    if isinstance(extra, dict):
        tf = _safe_str(extra.get("timeframe"))
        if tf:
            return tf
    return ""


def _normalize_decision_context(payload: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(payload)

    acct = _infer_account_label(out)
    sym = _infer_symbol(out)
    tf = _infer_timeframe(out)

    if not _safe_str(out.get("account_label")) and acct:
        out["account_label"] = acct
    if not _safe_str(out.get("symbol")) and sym:
        out["symbol"] = sym
    if not _safe_str(out.get("timeframe")) and tf:
        out["timeframe"] = tf

    if _safe_str(out.get("symbol")):
        out["symbol"] = _safe_upper(out.get("symbol"))

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
    try:
        # canonical pilot shape: schema_version=1 and decision present
        return d.get("schema_version") == 1 and ("decision" in d)
    except Exception:
        return False


def _is_legacy_pilot_spam(d: Dict[str, Any]) -> bool:
    """
    Legacy spam definition:
    - schema_version=1
    - has decision
    - has NO event_type
    - has NO meta.source
    These are the rows that historically flooded the store.
    """
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
    """
    HARD pilot dedupe: ONE row per (trade_id, account_label, symbol, timeframe).
    Ignore decision/reason/memory_fp because those are exactly what caused drift.
    """
    tid = _safe_str(d.get("trade_id"))
    acct = _safe_str(d.get("account_label"))
    sym = _safe_upper(d.get("symbol"))
    tf = _safe_str(d.get("timeframe"))
    return f"PILOT_CANON|{tid}|{acct}|{sym}|{tf}"


def _canonical_dedupe_key(d: Dict[str, Any]) -> str:
    # For pilot rows, use the hard key.
    if _is_pilot_row(d) or _safe_str(d.get("event_type")) == "pilot_decision":
        return _pilot_dedupe_key(d)

    trade_id = _safe_str(d.get("trade_id"))
    acct = _safe_str(d.get("account_label"))
    sym = _safe_upper(d.get("symbol"))
    tf = _safe_str(d.get("timeframe"))
    stage = _infer_stage(d)
    return f"CANON|{trade_id}|{stage}|{acct}|{sym}|{tf}"


def _dedupe_key(d: Dict[str, Any]) -> str:
    trade_id = _safe_str(d.get("trade_id"))
    acct = _safe_str(d.get("account_label"))
    sym = _safe_upper(d.get("symbol"))
    tf = _safe_str(d.get("timeframe"))

    # pilot rows: keep legacy key too, but canonical is PILOT_CANON above.
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
    h = hashlib.md5(orjson.dumps(d, option=orjson.OPT_SORT_KEYS, default=str)).hexdigest()
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
                    keys.add(_canonical_dedupe_key(d))
                    keys.add(_dedupe_key(d))
            except Exception:
                bad += 1
    except Exception:
        return keys, bad

    return keys, bad


def _append_bytes_atomic(path: Path, line_bytes: bytes) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("ab") as f:
            f.write(line_bytes)
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
    # top-level / common timestamps
    "ts", "ts_ms", "timestamp", "time_ms", "time",
    # bus freshness / ages
    "snapshot_age_sec", "snapshot_age_ms", "age_sec", "age_ms",
    # market bus update stamps
    "updated_ms", "orderbook_updated_ms", "trades_updated_ms",
    # orderbook/trades embedded stamps
    "ts_ms", "ts",
}

def _scrub_for_fp(x: Any) -> Any:
    """
    Recursively scrub volatile fields so snapshot_fp reflects meaningful state,
    not "current time" noise. Never throws.
    """
    try:
        if isinstance(x, dict):
            out: Dict[str, Any] = {}
            for k, v in x.items():
                ks = str(k)
                if ks in _VOLATILE_DROP_KEYS:
                    continue
                # defensive: drop any key ending with "_ms" that is clearly a clock stamp
                # (avoid nuking last_price etc. which are strings)
                if ks.endswith("_ms") and isinstance(v, (int, float, str)):
                    # keep numeric ms only if it's not obviously a time stamp? we drop all for fp stability
                    continue

                # Special-case: positions.raw is huge + noisy; fp should rely on by_symbol map instead.
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
    """
    sha256(canonical_json(scrubbed_snapshot))
    Never throws.
    """
    try:
        scrubbed = _scrub_for_fp(snapshot)
        s = _canon_json(scrubbed).encode("utf-8", errors="ignore")
        return hashlib.sha256(s).hexdigest()
    except Exception:
        return ""


def _snapshot_mode() -> str:
    # Mirror common EXEC_DRY_RUN semantics without importing flashback_common
    raw = os.getenv("EXEC_DRY_RUN", "false").strip().lower()
    return "DRY_RUN" if raw in ("1", "true", "yes", "y", "on") else "LIVE"


def _build_snapshot_for_payload(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Best-effort snapshot build. Never throws. Returns None on any failure.
    """
    try:
        from app.core import ai_state_bus  # local import to avoid coupling at module import time

        sym = _safe_upper(payload.get("symbol"))
        focus: Optional[List[str]] = [sym] if sym else None

        # Keep trades off by default (heavier + not required for most decisions)
        include_trades = _env_bool("AI_SNAPSHOT_INCLUDE_TRADES", "false")
        trades_limit = _env_int("AI_SNAPSHOT_TRADES_LIMIT", "50")

        include_orderbook = _env_bool("AI_SNAPSHOT_INCLUDE_ORDERBOOK", "true")

        snap = ai_state_bus.build_ai_snapshot(
            focus_symbols=focus,
            include_trades=include_trades,
            trades_limit=trades_limit,
            include_orderbook=include_orderbook,
        )
        if isinstance(snap, dict):
            return snap
        return None
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
    """
    Optional snapshot persistence to a separate store. Never throws.
    """
    try:
        if not fp:
            return
        if not _env_bool("AI_SNAPSHOTS_ENABLE", "false"):
            return

        spath = _snapshots_path()
        lockp = _snapshots_lock_path(spath)
        tail = _env_int("AI_SNAPSHOTS_DEDUPE_TAIL", "2500")
        warn_mb = _env_float("AI_SNAPSHOTS_WARN_MB", "25")
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

            try:
                if spath.exists():
                    size_mb = spath.stat().st_size / (1024 * 1024)
                    if size_mb >= warn_mb:
                        pass
            except Exception:
                pass
    except Exception:
        return


def _stamp_snapshot_linkage(payload: Dict[str, Any]) -> None:
    """
    Adds snapshot_fp + schema + mode to payload if enabled.
    Never throws.
    """
    try:
        if not _env_bool("AI_DECISIONS_STAMP_SNAPSHOT", "true"):
            return

        # If already present, don't stomp.
        if _safe_str(payload.get("snapshot_fp")):
            return

        snap = _build_snapshot_for_payload(payload)
        if not snap:
            # still stamp mode so downstream can see missing snapshot linkage
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
        tf = _safe_str(timeframe)

        if not tid or not acct or not sym:
            return

        if decision_exists(trade_id=tid, account_label=acct, symbol=sym):
            return

        payload: Dict[str, Any] = {
            "schema_version": 1,
            "ts_ms": _now_ms(),
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
            "timeframe": tf or "",
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

        warn_mb = _env_float("AI_DECISIONS_WARN_MB", "10")
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

        # ------------------------------------------------------------------
        # ✅ Canonical Decision Store Contract (Phase 4 determinism)
        # ------------------------------------------------------------------
        # Goal: one stable shape per (trade_id, account_label, stage, event_type)
        # - pilot_decision: schema_version=1
        # - ai_decision:    schema_version=2
        # Drop placeholder/junk rows before they hit the store.

        # Normalize / infer event_type if missing
        et = _safe_str(payload.get("event_type"))
        if not et:
            if _safe_str(payload.get("decision_code")) or _safe_str(payload.get("decision")):
                # Prefer ai_decision for decision_code-bearing rows; pilot rows are schema_version==1 with "decision"
                et = "ai_decision"
            payload["event_type"] = et

        # Ensure schema_version exists and is stable
        sv_raw = payload.get("schema_version", None)
        sv = _safe_int(sv_raw, default=0)
        et = _safe_str(payload.get("event_type"))

        if sv <= 0:
            if et == "pilot_decision":
                payload["schema_version"] = 1
            elif et == "ai_decision":
                payload["schema_version"] = 2
            else:
                # default to v2 unless explicitly pilot-tagged
                payload["schema_version"] = 2

        # Normalize decision_code from decision/payload
        dc = _safe_str(payload.get("decision_code"))
        d = _safe_str(payload.get("decision"))
        pl = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
        if not dc and isinstance(pl, dict):
            dc = _safe_str(pl.get("decision_code"))
        if not d and isinstance(pl, dict):
            d = _safe_str(pl.get("decision"))

        # If decision_code missing but decision present, copy it
        if not dc and d:
            payload["decision_code"] = d
            dc = d

        # Drop junk placeholders early
        if _safe_str(dc).upper() in ("NO_DECISION",):
            return

        # Force stage tag for stable dedupe behavior
        try:
            payload.setdefault("extra", {})
            if isinstance(payload["extra"], dict):
                stage = _safe_str(payload["extra"].get("stage"))
                if not stage:
                    # default stage by event type
                    payload["extra"]["stage"] = "pilot" if _safe_str(payload.get("event_type")) == "pilot_decision" else "enforced"
        except Exception:
            pass

        # --- pilot tagging: normalize legacy pilot input rows ---
        try:
            if payload.get("schema_version") == 1 and ("decision" in payload) and (not _safe_str(payload.get("event_type"))):
                payload["event_type"] = "pilot_decision"
        except Exception:
            pass

        # --- ts_ms stamping: missing OR None OR invalid ---
        try:
            ts_ms_raw = payload.get("ts_ms", None)
            ts_raw = payload.get("ts", None)
            ts_ms_i = _safe_int(ts_ms_raw, default=0)
            ts_i = _safe_int(ts_raw, default=0)

            if ts_ms_i <= 0 and ts_i <= 0:
                payload["ts_ms"] = _now_ms()
            elif ts_ms_i <= 0 and ts_i > 0:
                payload["ts_ms"] = ts_i
            elif ts_ms_i > 0:
                payload["ts_ms"] = ts_ms_i
        except Exception:
            payload["ts_ms"] = _now_ms()

        # Ensure pilot rows are always tagged
        if (_is_pilot_row(payload) or _safe_str(payload.get("event_type")) == "pilot_decision") and _safe_str(payload.get("event_type")) != "pilot_decision":
            payload["event_type"] = "pilot_decision"

        # Drop legacy spam pilot rows unless explicitly allowed
        if _is_legacy_pilot_spam(payload) and not allow_legacy_pilot:
            return

        # Reject/drop junk ai_decision rows missing both decision_code and decision
        et = _safe_str(payload.get("event_type"))
        if et == "ai_decision":
            dc = _safe_str(payload.get("decision_code"))
            d = _safe_str(payload.get("decision"))
            if not dc and not d:
                # Route to rejected if strict, otherwise drop silently.
                try:
                    payload.setdefault("extra", {})
                    if isinstance(payload["extra"], dict):
                        payload["extra"]["reject_reason"] = "ai_decision_missing_decision_code_and_decision"
                    line_rej = orjson.dumps(payload, option=orjson.OPT_SORT_KEYS, default=str) + b"\n"
                    _append_bytes_atomic(rejected_path, line_rej)
                except Exception:
                    pass
                return
            if not dc and d:
                payload["decision_code"] = d

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

            try:
                if path.exists():
                    size_mb = path.stat().st_size / (1024 * 1024)
                    if size_mb >= warn_mb:
                        pass
            except Exception:
                pass

    except Exception:
        return
