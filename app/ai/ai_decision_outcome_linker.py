#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Flashback — AI Decision ↔ Outcome Linker (Phase 4/5) v1.5

PATCH v1.5 (2025-12-29):
- Enforce FINAL-only outcome linking (non-final => SKIPPED_NON_FINAL, audited).
- Add restart-safe idempotency (persisted link index: trade_id↔outcome_id).
- Enforce 1:1 mapping (trade_id can only link once by default).
- Remove duplicated snapshot fields & duplicated integrity echoes.
- Keep existing decision indexing (ai_decision + pilot_decision + legacy heuristic).
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, List

import orjson


# -------------------------
# spine_api tolerant imports
# -------------------------
try:
    from app.core.spine_api import (
        STATE_DIR as _STATE_DIR,
        AI_EVENTS_DIR as _AI_EVENTS_DIR,
        AI_DECISIONS_PATH as _AI_DECISIONS_PATH,
        read_jsonl_tail,
        safe_str,
    )
except Exception:  # pragma: no cover
    _STATE_DIR = None  # type: ignore
    _AI_EVENTS_DIR = None  # type: ignore
    _AI_DECISIONS_PATH = None  # type: ignore
    read_jsonl_tail = None  # type: ignore
    def safe_str(x: Any) -> str:  # type: ignore
        try:
            return ('' if x is None else str(x)).strip()
        except Exception:
            return ''


# -------------------------
# ROOT + paths (tolerant)
# -------------------------
try:
    from app.core.config import settings  # type: ignore
    ROOT: Path = settings.ROOT  # type: ignore
except Exception:  # pragma: no cover
    ROOT = Path(__file__).resolve().parents[2]

STATE_DIR: Path = (_STATE_DIR if _STATE_DIR is not None else (ROOT / "state"))
AI_EVENTS_DIR: Path = (_AI_EVENTS_DIR if _AI_EVENTS_DIR is not None else (STATE_DIR / "ai_events"))
DECISIONS_PATH: Path = (_AI_DECISIONS_PATH if _AI_DECISIONS_PATH is not None else (STATE_DIR / "ai_decisions.jsonl"))

OUTCOMES_PATH: Path = AI_EVENTS_DIR / "outcomes.v1.jsonl"
OUT_PATH: Path = STATE_DIR / "ai_decision_outcomes.v1.jsonl"
CURSOR_PATH: Path = STATE_DIR / "ai_decision_outcome_cursor.json"
LINK_INDEX_PATH: Path = STATE_DIR / "ai_decision_outcome_link_index.json"  # NEW (idempotency)

STATE_DIR.mkdir(parents=True, exist_ok=True)
AI_EVENTS_DIR.mkdir(parents=True, exist_ok=True)
OUT_PATH.parent.mkdir(parents=True, exist_ok=True)


# -------------------------
# utils
# -------------------------
def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        return int(x)
    except Exception:
        return default


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _safe_str(x: Any) -> str:
    try:
        if x is None:
            return ""
        return str(x).strip()
    except Exception:
        return ""


def _read_json(path: Path, default: Dict[str, Any]) -> Dict[str, Any]:
    try:
        if not path.exists():
            return dict(default)
        data = orjson.loads(path.read_bytes())
        return data if isinstance(data, dict) else dict(default)
    except Exception:
        return dict(default)


def _write_json(path: Path, obj: Dict[str, Any]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(orjson.dumps(obj, option=orjson.OPT_INDENT_2))
    except Exception:
        pass


def _append_jsonl(path: Path, obj: Dict[str, Any]) -> None:
    """
    Append JSONL safely (this writes to OUT_PATH, NOT ai_decisions.jsonl).
    """
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        line = orjson.dumps(obj) + b"\n"
        import os as _os
        fd = _os.open(str(path), _os.O_APPEND | _os.O_CREAT | _os.O_WRONLY, 0o666)
        try:
            _os.write(fd, line)
        finally:
            _os.close(fd)
    except Exception:
        pass


# -------------------------
# extraction helpers
# -------------------------
def _decision_all_trade_ids(d: Dict[str, Any]) -> List[str]:
    ids = set()
    for k in ("trade_id", "client_trade_id", "source_trade_id"):
        v = d.get(k)
        if isinstance(v, str) and v.strip():
            ids.add(v.strip())
    return list(ids)


def _extract_trade_id(evt: Dict[str, Any]) -> str:
    for k in ("trade_id", "client_trade_id", "source_trade_id"):
        tid = evt.get(k)
        if isinstance(tid, str) and tid.strip():
            return tid.strip()

    setup = evt.get("setup")
    if isinstance(setup, dict):
        tid2 = setup.get("trade_id")
        if isinstance(tid2, str) and tid2.strip():
            return tid2.strip()

    ctx = evt.get("setup_context")
    if isinstance(ctx, dict):
        tid3 = ctx.get("trade_id")
        if isinstance(tid3, str) and tid3.strip():
            return tid3.strip()

    return ""


def _extract_symbol(evt: Dict[str, Any]) -> str:
    sym = evt.get("symbol")
    if isinstance(sym, str) and sym.strip():
        return sym.strip().upper()

    setup = evt.get("setup")
    if isinstance(setup, dict):
        sym2 = setup.get("symbol")
        if isinstance(sym2, str) and sym2.strip():
            return sym2.strip().upper()

    ctx = evt.get("setup_context")
    if isinstance(ctx, dict):
        sym3 = ctx.get("symbol")
        if isinstance(sym3, str) and sym3.strip():
            return sym3.strip().upper()

    return ""


def _extract_account_label(evt: Dict[str, Any]) -> str:
    for k in ("account_label", "account", "subaccount", "accountName"):
        v = evt.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()

    setup = evt.get("setup")
    if isinstance(setup, dict):
        v2 = setup.get("account_label")
        if isinstance(v2, str) and v2.strip():
            return v2.strip()

    ctx = evt.get("setup_context")
    if isinstance(ctx, dict):
        v3 = ctx.get("account_label")
        if isinstance(v3, str) and v3.strip():
            return v3.strip()

    return ""


def _extract_outcome_id(evt: Dict[str, Any]) -> str:
    oid = evt.get("outcome_id")
    if isinstance(oid, str) and oid.strip():
        return oid.strip()

    oid2 = evt.get("id")
    if isinstance(oid2, str) and oid2.strip():
        return oid2.strip()

    tid = _extract_trade_id(evt)
    ts = _safe_str(evt.get("ts") or evt.get("ts_ms") or evt.get("closed_ts_ms") or "")
    pnl = _safe_str(evt.get("pnl_usd") or (evt.get("payload") or {}).get("pnl_usd") or "")
    reason = _safe_str(evt.get("close_reason") or evt.get("exit_reason") or (evt.get("payload") or {}).get("close_reason") or "")
    fp = f"{tid}|{ts}|{pnl}|{reason}".strip("|")
    return fp if fp else ""


def _extract_snapshot_linkage(d: Dict[str, Any]) -> Dict[str, Any]:
    try:
        fp = d.get("snapshot_fp")
        mode = d.get("snapshot_mode")
        sv = d.get("snapshot_schema_version")
        return {
            "snapshot_fp": _safe_str(fp) or None,
            "snapshot_mode": _safe_str(mode) or None,
            "snapshot_schema_version": _safe_int(sv, 0) if sv is not None else None,
        }
    except Exception:
        return {"snapshot_fp": None, "snapshot_mode": None, "snapshot_schema_version": None}


def _decision_symbol(d: Dict[str, Any]) -> str:
    return _safe_str(d.get("symbol")).upper()


def _decision_account_label(d: Dict[str, Any]) -> str:
    return _safe_str(d.get("account_label"))


def _decision_ts_ms(d: Dict[str, Any]) -> int:
    ts_ms = d.get("ts_ms")
    if ts_ms is not None:
        return _safe_int(ts_ms, 0)
    ts = d.get("ts")
    if ts is not None:
        return _safe_int(ts, 0)
    meta = d.get("meta")
    if isinstance(meta, dict):
        if meta.get("ts_ms") is not None:
            return _safe_int(meta.get("ts_ms"), 0)
        if meta.get("ts") is not None:
            return _safe_int(meta.get("ts"), 0)
    return 0


def _looks_like_decision_row(d: Dict[str, Any]) -> bool:
    et = d.get("event_type")
    if et in ("ai_decision", "pilot_decision"):
        return True

    if et is not None:
        return False

    tid = d.get("trade_id")
    dec = d.get("decision") or d.get("decision_code")
    if not (isinstance(tid, str) and tid.strip()):
        return False
    if dec is None:
        return False

    for k in ("allow", "size_multiplier", "gates", "policy_hash", "tier_used", "meta"):
        if k in d and d.get(k) is not None:
            return True
    return False


# -------------------------
# FINALITY gate (Outcome side) - CANONICAL
# -------------------------
def _is_final_outcome(o: Dict[str, Any]) -> bool:
    """
    FINALITY RULE (canonical):
    - outcomes.v1.jsonl rows are terminal when they have close_reason + a close timestamp field.
    - outcome.v1 uses closed_ts_ms (your data does).
    - Synthetic terminal statuses (ABORTED/EXPIRED/CLOSED/DONE/FINAL) are final.
    - Explicit non-terminal statuses (OPEN/PARTIAL/PENDING/WORKING/FILL_EVENT) are not final.
    """
    if not isinstance(o, dict):
        return False

    payload = o.get("payload") if isinstance(o.get("payload"), dict) else {}

    if o.get("is_final") is True:
        return True

    schema = str(o.get("schema_version") or payload.get("schema_version") or "").strip()

    close_reason = str(
        o.get("close_reason")
        or payload.get("close_reason")
        or o.get("exit_reason")
        or payload.get("exit_reason")
        or ""
    ).strip()

    closed_ts = (
        o.get("closed_ts_ms")
        or o.get("closed_ts")
        or o.get("close_ts")
        or o.get("exit_ts")
        or o.get("ts_close")
        or payload.get("closed_ts_ms")
        or payload.get("closed_ts")
        or payload.get("close_ts")
        or payload.get("exit_ts")
        or payload.get("ts_close")
    )

    final_status = str(
        o.get("final_status") or payload.get("final_status") or o.get("status") or payload.get("status") or ""
    ).strip().upper()

    if final_status in ("ABORTED", "EXPIRED", "CLOSED", "DONE", "FINAL"):
        return True
    if final_status in ("OPEN", "PARTIAL", "PENDING", "WORKING", "FILL_EVENT"):
        return False

    if schema == "outcome.v1":
        return bool(close_reason) and (closed_ts is not None)

    pnl = o.get("pnl_usd") if o.get("pnl_usd") is not None else payload.get("pnl_usd")
    if schema.startswith("outcome.") and close_reason and pnl is not None:
        return True

    return False


# -------------------------
# decisions index
# -------------------------
class DecisionIndex:
    def __init__(self, path: Path):
        self.path = path
        self._cache: Dict[str, List[Dict[str, Any]]] = {}
        self._last_sig: Tuple[int, int] = (0, 0)

    def _sig(self) -> Tuple[int, int]:
        try:
            st = self.path.stat()
            return (getattr(st, "st_mtime_ns", int(st.st_mtime * 1e9)), st.st_size)
        except Exception:
            return (0, 0)

    def maybe_reload(self) -> None:
        sig = self._sig()
        if sig == self._last_sig:
            return
        self._last_sig = sig
        self._cache = self._load_all_by_trade_id()

    def _load_all_by_trade_id(self) -> Dict[str, List[Dict[str, Any]]]:
        out: Dict[str, List[Dict[str, Any]]] = {}
        if not self.path.exists():
            return out

        try:
            with self.path.open("rb") as f:
                for line in f:
                    raw = line.strip()
                    if not raw or raw[:1] != b"{":
                        continue
                    try:
                        d = orjson.loads(raw)
                    except Exception:
                        continue
                    if not isinstance(d, dict):
                        continue
                    if not _looks_like_decision_row(d):
                        continue

                    tids = _decision_all_trade_ids(d)
                    if not tids:
                        continue

                    for tid in tids:
                        out.setdefault(tid, []).append(d)

        except Exception:
            return out

        for tid, arr in out.items():
            arr.sort(key=_decision_ts_ms)
            out[tid] = arr
        return out

    def _pick_latest(self, arr: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        return arr[-1] if arr else None

    def get_best_for_outcome(self, trade_id: str, account_label: str, symbol: str) -> Tuple[Optional[Dict[str, Any]], int, str]:
        if not trade_id:
            return None, 0, "no_decision"

        self.maybe_reload()
        candidates = self._cache.get(trade_id) or []
        if not candidates:
            return None, 0, "no_decision"

        acct = _safe_str(account_label)
        sym = _safe_str(symbol).upper()

        if acct and sym:
            bucket = [d for d in candidates if _decision_account_label(d) == acct and _decision_symbol(d) == sym]
            best = self._pick_latest(bucket)
            if best:
                return best, 1, "tid+acct+sym"

        if acct:
            bucket = [d for d in candidates if _decision_account_label(d) == acct]
            best = self._pick_latest(bucket)
            if best:
                return best, 2, "tid+acct"

        return self._pick_latest(candidates), 3, "tid_only"


# -------------------------
# cursor / streaming reader
# -------------------------
def _load_cursor() -> Dict[str, Any]:
    return _read_json(CURSOR_PATH, {"offset": 0, "updated_ms": 0})


def _save_cursor(offset: int) -> None:
    _write_json(CURSOR_PATH, {"offset": int(offset), "updated_ms": _now_ms()})


def _read_new_jsonl(path: Path, offset: int) -> Tuple[List[Dict[str, Any]], int]:
    if not path.exists():
        return ([], offset)

    data: List[Dict[str, Any]] = []
    try:
        with path.open("rb") as f:
            f.seek(max(0, int(offset)))
            while True:
                line = f.readline()
                if not line:
                    break
                raw = line.strip()
                if not raw or raw[:1] != b"{":
                    continue
                try:
                    obj = orjson.loads(raw)
                except Exception:
                    continue
                if isinstance(obj, dict):
                    data.append(obj)
            new_off = f.tell()
            return (data, new_off)
    except Exception:
        return ([], offset)


# -------------------------
# link index (idempotency)
# -------------------------
def _load_link_index() -> Dict[str, Any]:
    default = {"version": 1, "updated_ms": 0, "by_trade_id": {}, "by_outcome_id": {}}
    idx = _read_json(LINK_INDEX_PATH, default)
    if not isinstance(idx.get("by_trade_id"), dict):
        idx["by_trade_id"] = {}
    if not isinstance(idx.get("by_outcome_id"), dict):
        idx["by_outcome_id"] = {}
    return idx


def _save_link_index(idx: Dict[str, Any]) -> None:
    idx["updated_ms"] = _now_ms()

    MAX = 50000
    by_tid: Dict[str, Any] = idx.get("by_trade_id", {})  # type: ignore[assignment]
    by_oid: Dict[str, Any] = idx.get("by_outcome_id", {})  # type: ignore[assignment]
    if isinstance(by_tid, dict) and len(by_tid) > MAX:
        drop = len(by_tid) - MAX
        for k in list(by_tid.keys())[:drop]:
            old_oid = by_tid.pop(k, None)
            if old_oid and isinstance(by_oid, dict):
                by_oid.pop(old_oid, None)
    if isinstance(by_oid, dict) and len(by_oid) > MAX:
        drop = len(by_oid) - MAX
        for k in list(by_oid.keys())[:drop]:
            old_tid = by_oid.pop(k, None)
            if old_tid and isinstance(by_tid, dict):
                by_tid.pop(old_tid, None)

    _write_json(LINK_INDEX_PATH, idx)


# -------------------------
# summarize decision / outcome
# -------------------------
def _summarize_decision(d: Dict[str, Any]) -> Dict[str, Any]:
    gates = d.get("gates") if isinstance(d.get("gates"), dict) else {}
    mem = d.get("memory") if isinstance(d.get("memory"), dict) else None
    prop = d.get("proposed_action") if isinstance(d.get("proposed_action"), dict) else None

    decision_str = d.get("decision")
    if decision_str is None:
        decision_str = d.get("decision_code")

    policy_hash = d.get("policy_hash")
    if not policy_hash and isinstance(d.get("policy"), dict):
        policy_hash = (d.get("policy") or {}).get("policy_hash")

    snap = _extract_snapshot_linkage(d)

    return {
        "schema_version": _safe_int(d.get("schema_version"), 1),
        "ts_ms": _decision_ts_ms(d),
        "decision": decision_str,
        "allow": bool(d.get("allow")) if d.get("allow") is not None else None,
        "size_multiplier": _safe_float(d.get("size_multiplier"), 1.0) if d.get("size_multiplier") is not None else None,
        "tier_used": d.get("tier_used") or d.get("tier"),
        "gates_reason": gates.get("reason") if isinstance(gates, dict) else None,
        "gates": gates if isinstance(gates, dict) else {},
        "memory_id": (mem.get("memory_id") if mem else None),
        "memory_score": (_safe_float(mem.get("score"), 0.0) if mem else None),
        "proposed_action": prop,
        "policy_hash": policy_hash,
        "account_label": _decision_account_label(d),
        "symbol": _decision_symbol(d),
        "event_type": d.get("event_type"),

        "snapshot_fp": snap.get("snapshot_fp"),
        "snapshot_mode": snap.get("snapshot_mode"),
        "snapshot_schema_version": snap.get("snapshot_schema_version"),
    }


def _summarize_outcome(o: Dict[str, Any]) -> Dict[str, Any]:
    stats = o.get("stats") if isinstance(o.get("stats"), dict) else {}
    payload = o.get("payload") if isinstance(o.get("payload"), dict) else {}

    pnl = o.get("pnl_usd")
    if pnl is None:
        pnl = payload.get("pnl_usd")
    if pnl is None:
        pnl = stats.get("pnl_usd")

    r_mult = stats.get("r_multiple")
    if r_mult is None:
        r_mult = payload.get("r_multiple")
    if r_mult is None:
        r_mult = o.get("r_multiple")

    win = stats.get("win")
    if win is None:
        win = payload.get("win")
    if win is None:
        win = o.get("win")

    close_reason = o.get("close_reason") or payload.get("close_reason") or o.get("exit_reason") or payload.get("exit_reason")
    final_status = o.get("final_status") or payload.get("final_status") or o.get("status")

    return {
        "event_type": o.get("event_type"),
        "ts": _safe_int(o.get("ts"), 0) or _safe_int(o.get("ts_ms"), 0),
        "symbol": _extract_symbol(o),
        "account_label": _extract_account_label(o),
        "trade_id": _extract_trade_id(o),
        "outcome_id": _extract_outcome_id(o),
        "is_final": bool(_is_final_outcome(o)),
        "pnl_usd": _safe_float(pnl, 0.0),
        "r_multiple": r_mult,
        "win": win,
        "close_reason": close_reason,
        "final_status": final_status,
    }


def _join(
    decision: Optional[Dict[str, Any]],
    outcome: Dict[str, Any],
    *,
    match_level: int,
    match_rule: str,
    status: str,
    quarantine: bool,
) -> Dict[str, Any]:
    tid = _extract_trade_id(outcome)
    sym = _extract_symbol(outcome)
    acct = _extract_account_label(outcome)

    decision_summary = _summarize_decision(decision) if decision else None
    snap_fp = decision_summary.get("snapshot_fp") if decision_summary else None
    snap_mode = decision_summary.get("snapshot_mode") if decision_summary else None
    snap_sv = decision_summary.get("snapshot_schema_version") if decision_summary else None

    out_sum = _summarize_outcome(outcome)

    return {
        "ts_ms": _now_ms(),
        "status": status,
        "quarantine": bool(quarantine),

        "trade_id": tid,
        "symbol": sym,
        "account_label": acct,

        "snapshot_fp": snap_fp,
        "snapshot_mode": snap_mode,
        "snapshot_schema_version": snap_sv,

        "match_level": int(match_level) if decision else 0,
        "match_rule": str(match_rule) if decision else "no_decision",

        "decision": decision_summary,
        "outcome": out_sum,

        "integrity": {
            "decision_present": bool(decision),
            "final_outcome": bool(out_sum.get("is_final")),
            "linked_at_ms": _now_ms(),
        },
    }


# -------------------------
# processing
# -------------------------
def process_once(idx: DecisionIndex) -> Dict[str, Any]:
    cursor = _load_cursor()
    offset = _safe_int(cursor.get("offset"), 0)

    events, new_offset = _read_new_jsonl(OUTCOMES_PATH, offset)

    link_idx = _load_link_index()
    by_tid: Dict[str, str] = link_idx.get("by_trade_id", {})  # type: ignore[assignment]
    by_oid: Dict[str, str] = link_idx.get("by_outcome_id", {})  # type: ignore[assignment]

    written = 0
    skipped_non_final = 0
    missing_trade_id = 0
    no_decision_found = 0
    duplicates = 0

    for evt in events:
        tid = _extract_trade_id(evt)
        sym = _extract_symbol(evt)
        acct = _extract_account_label(evt)
        oid = _extract_outcome_id(evt)

        if not tid:
            missing_trade_id += 1
            joined = _join(None, evt, match_level=0, match_rule="no_decision", status="MISSING_TRADE_ID", quarantine=True)
            _append_jsonl(OUT_PATH, joined)
            written += 1
            continue

        if not _is_final_outcome(evt):
            skipped_non_final += 1
            joined = _join(None, evt, match_level=0, match_rule="non_final", status="SKIPPED_NON_FINAL", quarantine=True)
            _append_jsonl(OUT_PATH, joined)
            written += 1
            continue

        if not oid:
            joined = _join(None, evt, match_level=0, match_rule="missing_outcome_id", status="MISSING_OUTCOME_ID", quarantine=True)
            _append_jsonl(OUT_PATH, joined)
            written += 1
            continue

        if isinstance(by_oid, dict) and oid in by_oid:
            duplicates += 1
            joined = _join(None, evt, match_level=0, match_rule="dup_outcome_id", status="DUPLICATE_OUTCOME_ID", quarantine=True)
            _append_jsonl(OUT_PATH, joined)
            written += 1
            continue

        if isinstance(by_tid, dict) and tid in by_tid:
            duplicates += 1
            joined = _join(None, evt, match_level=0, match_rule="dup_trade_id", status="DUPLICATE_TRADE_ID", quarantine=True)
            _append_jsonl(OUT_PATH, joined)
            written += 1
            continue

        dec, match_level, match_rule = idx.get_best_for_outcome(tid, acct, sym)
        if not dec:
            no_decision_found += 1
            joined = _join(None, evt, match_level=0, match_rule="no_decision", status="NO_DECISION_FOUND", quarantine=True)
            _append_jsonl(OUT_PATH, joined)
            written += 1
            continue

        joined = _join(dec, evt, match_level=match_level, match_rule=match_rule, status="OK", quarantine=False)
        _append_jsonl(OUT_PATH, joined)
        written += 1

        if isinstance(by_tid, dict):
            by_tid[tid] = oid
        if isinstance(by_oid, dict):
            by_oid[oid] = tid

    _save_cursor(new_offset)
    _save_link_index(link_idx)

    return {
        "ok": True,
        "ts_ms": _now_ms(),
        "outcomes_seen": len(events),
        "written": written,
        "skipped_non_final": skipped_non_final,
        "missing_trade_id": missing_trade_id,
        "no_decision_found": no_decision_found,
        "duplicates": duplicates,
        "cursor_offset_before": offset,
        "cursor_offset_after": new_offset,
        "paths": {
            "decisions": str(DECISIONS_PATH),
            "outcomes": str(OUTCOMES_PATH),
            "out": str(OUT_PATH),
            "cursor": str(CURSOR_PATH),
            "link_index": str(LINK_INDEX_PATH),
        },
    }


def loop(poll_seconds: float) -> None:
    idx = DecisionIndex(DECISIONS_PATH)
    while True:
        report = process_once(idx)
        print(
            f"[ai_decision_outcome_linker] seen={report['outcomes_seen']} written={report['written']} "
            f"non_final={report['skipped_non_final']} missing_tid={report['missing_trade_id']} "
            f"no_decision={report['no_decision_found']} dup={report['duplicates']} "
            f"offset={report['cursor_offset_after']}"
        )
        time.sleep(max(0.25, poll_seconds))


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--once", action="store_true", help="Process new outcomes once and exit")
    p.add_argument("--poll", type=float, default=2.0, help="Polling seconds for loop mode")
    args = p.parse_args()

    idx = DecisionIndex(DECISIONS_PATH)

    if args.once:
        report = process_once(idx)
        print(orjson.dumps(report, option=orjson.OPT_INDENT_2).decode("utf-8"))
        return 0

    loop(args.poll)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
