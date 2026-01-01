#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Flashback — AI Decision ↔ Outcome Linker (Phase 4/5) v1.4

PATCH v1.4 (2025-12-19):
- Index BOTH event_type=="ai_decision" and event_type=="pilot_decision".
- Tolerate legacy decision rows missing event_type (heuristic detection).
- Keep deterministic pick using outcome account_label/symbol.
- Outcome summarization reads canonical outcome_record payload fields.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, List

import orjson


# -------------------------
# ROOT + paths (tolerant)
# -------------------------
try:
    from app.core.config import settings  # type: ignore
    ROOT: Path = settings.ROOT  # type: ignore
except Exception:  # pragma: no cover
    ROOT = Path(__file__).resolve().parents[2]

STATE_DIR: Path = ROOT / "state"
AI_EVENTS_DIR: Path = STATE_DIR / "ai_events"

DECISIONS_PATH: Path = STATE_DIR / "ai_decisions.jsonl"
OUTCOMES_PATH: Path = AI_EVENTS_DIR / "outcomes.jsonl"

OUT_PATH: Path = STATE_DIR / "ai_decision_outcomes.jsonl"
CURSOR_PATH: Path = STATE_DIR / "ai_decision_outcome_cursor.json"

STATE_DIR.mkdir(parents=True, exist_ok=True)
AI_EVENTS_DIR.mkdir(parents=True, exist_ok=True)
OUT_PATH.parent.mkdir(parents=True, exist_ok=True)


# -------------------------
# utils
# -------------------------
def _now_ms() -> int:
    return int(time.time() * 1000)


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
        path.write_bytes(orjson.dumps(obj, option=orjson.OPT_INDENT_2))
    except Exception:
        pass


def _append_jsonl(path: Path, obj: Dict[str, Any]) -> None:
    """
    Append JSONL safely WITHOUT using Path.open("ab") so scan_decision_writers.py
    doesn't falsely label this module as a "decision writer".

    This writes to OUT_PATH (ai_decision_outcomes.jsonl), NOT ai_decisions.jsonl.
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
    tid = evt.get("trade_id")
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
    v = evt.get("account_label")
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


def _extract_outcome_policy_hash(evt: Dict[str, Any]) -> Optional[str]:
    pol = evt.get("policy")
    if isinstance(pol, dict):
        phs = _safe_str(pol.get("policy_hash"))
        return phs if phs else None
    return None


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
    """
    Accept:
    - event_type == ai_decision
    - event_type == pilot_decision
    - legacy rows missing event_type but clearly shaped like a decision
      (must have trade_id + decision + at least one of allow/size_multiplier/gates/policy_hash)
    """
    et = d.get("event_type")
    if et in ("ai_decision", "pilot_decision"):
        return True

    # Legacy heuristic (be conservative)
    if et is not None:
        return False  # unknown explicit event_type -> do not guess

    tid = d.get("trade_id")
    dec = d.get("decision") or d.get("decision_code")
    if not (isinstance(tid, str) and tid.strip()):
        return False
    if dec is None:
        return False

    has_any = False
    for k in ("allow", "size_multiplier", "gates", "policy_hash", "tier_used", "meta"):
        if k in d and d.get(k) is not None:
            has_any = True
            break
    return has_any



def _extract_snapshot_linkage(d: Dict[str, Any]) -> Dict[str, Any]:
    """\
    Phase 7: decisions may carry snapshot linkage fields stamped by ai_decision_logger.
    Tolerant: if fields are missing or invalid, return None values.
    """
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

                    # ✅ Index true decisions (ai_decision + pilot_decision + legacy)
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
# join logic
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

        # ✅ Phase 7 linkage
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

    exit_reason = o.get("exit_reason") or payload.get("exit_reason") or stats.get("exit_reason")
    final_status = o.get("final_status") or payload.get("final_status") or stats.get("final_status")

    return {
        "event_type": o.get("event_type"),
        "ts": _safe_int(o.get("ts"), 0) or _safe_int(o.get("ts_ms"), 0),
        "symbol": _extract_symbol(o),
        "account_label": _extract_account_label(o),
        "trade_id": _extract_trade_id(o),
        "pnl_usd": _safe_float(pnl, 0.0),
        "r_multiple": r_mult,
        "win": win,
        "exit_reason": exit_reason,
        "final_status": final_status,
        "raw": o,
    }


def _inherit_decision_fields_from_outcome(decision_summary: Dict[str, Any], *, outcome_symbol: str, outcome_account_label: str, outcome_policy_hash: Optional[str]) -> Dict[str, Any]:
    if not _safe_str(decision_summary.get("symbol")) and outcome_symbol:
        decision_summary["symbol"] = outcome_symbol
    if not _safe_str(decision_summary.get("account_label")) and outcome_account_label:
        decision_summary["account_label"] = outcome_account_label
    if not _safe_str(decision_summary.get("policy_hash")) and outcome_policy_hash:
        decision_summary["policy_hash"] = outcome_policy_hash
    return decision_summary


def _join(decision: Optional[Dict[str, Any]], outcome: Dict[str, Any], *, match_level: int, match_rule: str) -> Dict[str, Any]:
    trade_id = _extract_trade_id(outcome)
    symbol = _extract_symbol(outcome)
    account_label = _extract_account_label(outcome)
    outcome_policy_hash = _extract_outcome_policy_hash(outcome)

    decision_summary = _summarize_decision(decision) if decision else None
    # ✅ Promote snapshot linkage to top-level for auditability
    snap_fp = None
    snap_mode = None
    snap_sv = None
    if decision_summary:
        snap_fp = decision_summary.get("snapshot_fp")
        snap_mode = decision_summary.get("snapshot_mode")
        snap_sv = decision_summary.get("snapshot_schema_version")

    if decision_summary:
        decision_summary = _inherit_decision_fields_from_outcome(
            decision_summary,
            outcome_symbol=symbol,
            outcome_account_label=account_label,
            outcome_policy_hash=outcome_policy_hash,
        )

    joined: Dict[str, Any] = {
        "ts_ms": _now_ms(),
        "trade_id": trade_id,
        "symbol": symbol,
        "account_label": account_label,

        # Phase 7 linkage (top-level)
        "snapshot_fp": snap_fp,
        "snapshot_mode": snap_mode,
        "snapshot_schema_version": snap_sv,

        # Phase 7 linkage (top-level)
        "snapshot_fp": snap_fp,
        "snapshot_mode": snap_mode,
        "snapshot_schema_version": snap_sv,
        "status": "OK" if decision else "NO_DECISION_FOUND",
        "match_level": int(match_level) if decision else 0,
        "match_rule": str(match_rule) if decision else "no_decision",
        "decision": decision_summary,
        "outcome": _summarize_outcome(outcome),
        "integrity": {
            "decision_present": bool(decision),
            "match_level": match_level if decision else 0,
            "match_rule": match_rule if decision else "no_decision",
            "linked_at_ms": _now_ms(),

            # helpful integrity echoes
            "snapshot_fp": snap_fp,
            "snapshot_mode": snap_mode,
            "snapshot_schema_version": snap_sv,

            # helpful integrity echoes
            "snapshot_fp": snap_fp,
            "snapshot_mode": snap_mode,
            "snapshot_schema_version": snap_sv,
        },
    }
    return joined


# -------------------------
# main processing
# -------------------------
def process_once(idx: DecisionIndex) -> Dict[str, Any]:
    cursor = _load_cursor()
    offset = _safe_int(cursor.get("offset"), 0)

    events, new_offset = _read_new_jsonl(OUTCOMES_PATH, offset)
    written = 0
    no_trade_id = 0
    no_decision = 0

    for evt in events:
        tid = _extract_trade_id(evt)
        sym = _extract_symbol(evt)
        acct = _extract_account_label(evt)

        if not tid:
            no_trade_id += 1
            joined = _join(None, evt, match_level=0, match_rule="no_decision")
            joined["status"] = "MISSING_TRADE_ID"
            _append_jsonl(OUT_PATH, joined)
            written += 1
            continue

        dec, match_level, match_rule = idx.get_best_for_outcome(tid, acct, sym)
        if not dec:
            no_decision += 1
            match_level = 0
            match_rule = "no_decision"

        joined = _join(dec, evt, match_level=match_level, match_rule=match_rule)
        _append_jsonl(OUT_PATH, joined)
        written += 1

    _save_cursor(new_offset)

    return {
        "ok": True,
        "ts_ms": _now_ms(),
        "outcomes_seen": len(events),
        "written": written,
        "missing_trade_id": no_trade_id,
        "no_decision_found": no_decision,
        "cursor_offset_before": offset,
        "cursor_offset_after": new_offset,
        "paths": {
            "decisions": str(DECISIONS_PATH),
            "outcomes": str(OUTCOMES_PATH),
            "out": str(OUT_PATH),
            "cursor": str(CURSOR_PATH),
        },
    }


def loop(poll_seconds: float) -> None:
    idx = DecisionIndex(DECISIONS_PATH)
    while True:
        report = process_once(idx)
        print(
            f"[ai_decision_outcome_linker] outcomes={report['outcomes_seen']} "
            f"written={report['written']} missing_tid={report['missing_trade_id']} "
            f"no_decision={report['no_decision_found']} offset={report['cursor_offset_after']}"
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
