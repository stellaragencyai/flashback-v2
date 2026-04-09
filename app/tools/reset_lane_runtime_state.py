#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

try:
    import yaml  # type: ignore
except Exception:
    yaml = None  # type: ignore


THIS_FILE = Path(__file__).resolve()
REPO_ROOT = THIS_FILE.parents[2]
STATE_ROOT = REPO_ROOT / "state"
SIGNALS_ROOT = REPO_ROOT / "signals"
PAPER_ROOT = STATE_ROOT / "paper"
DEFAULT_LABELS = [f"flashback{idx:02d}" for idx in range(1, 10)]
SUBACCOUNTS_PATH = REPO_ROOT / "config" / "subaccounts.yaml"
GLOBAL_AI_EVENTS_DIR = STATE_ROOT / "ai_events"
SHARED_OUTCOME_FILES = [
    GLOBAL_AI_EVENTS_DIR / "outcomes.v1.jsonl",
    GLOBAL_AI_EVENTS_DIR / "outcomes.jsonl",
    GLOBAL_AI_EVENTS_DIR / "outcomes_raw.jsonl",
    GLOBAL_AI_EVENTS_DIR / "outcomes_orphans.jsonl",
    STATE_ROOT / "ai_decision_outcomes.v1.jsonl",
]


def _safe_read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8", errors="ignore"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _safe_read_yaml(path: Path) -> Dict[str, Any]:
    if yaml is None or not path.exists():
        return {}
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8", errors="ignore")) or {}
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.tmp.{int(time.time() * 1000)}")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def _atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    _atomic_write_text(path, json.dumps(payload, indent=2, sort_keys=True))


def _now_tag() -> str:
    return time.strftime("%Y%m%d_%H%M%S", time.localtime())


def _lane_enabled_map() -> Dict[str, bool]:
    cfg = _safe_read_yaml(SUBACCOUNTS_PATH)
    out: Dict[str, bool] = {}
    for label, node in cfg.items():
        if label in {"version", "notes", "legacy"} or not isinstance(node, dict):
            continue
        out[str(label)] = bool(node.get("enabled", True))
    return out


def _selected_labels(raw_labels: Iterable[str], include_disabled: bool) -> List[str]:
    requested = [str(v).strip().lower() for v in raw_labels if str(v).strip()]
    if not requested:
        requested = list(DEFAULT_LABELS)
    enabled_map = _lane_enabled_map()
    labels: List[str] = []
    for label in requested:
        if not include_disabled and label in enabled_map and not enabled_map[label]:
            continue
        labels.append(label)
    return sorted(list(dict.fromkeys(labels)))


def _lane_archive_dir(tag: str, label: str) -> Path:
    return STATE_ROOT / "runtime_resets" / tag / label


def _archive_path(src: Path, archive_root: Path) -> None:
    if not src.exists():
        return
    archive_root.mkdir(parents=True, exist_ok=True)
    dst = archive_root / src.name
    if src.is_dir():
        if dst.exists():
            shutil.rmtree(dst, ignore_errors=True)
        shutil.copytree(src, dst)
    else:
        shutil.copy2(src, dst)


def _candidate_paths(label: str) -> List[Path]:
    lane = str(label).strip().lower()
    paths = [
        PAPER_ROOT / f"{lane}.json",
        STATE_ROOT / f"positions_bus_{lane}.json",
        STATE_ROOT / f"orderbook_bus_{lane}.json",
        STATE_ROOT / f"trades_bus_{lane}.json",
        STATE_ROOT / f"public_trades_{lane}.jsonl",
        STATE_ROOT / f"ws_executions_{lane}.jsonl",
        STATE_ROOT / f"trade_outcome_recorder_{lane}.cursor",
        STATE_ROOT / f"ai_decisions_{lane}.jsonl",
        STATE_ROOT / f"ai_decisions_{lane}.jsonl.lock",
        STATE_ROOT / f"ai_events_inbox_{lane}.jsonl",
        STATE_ROOT / f"ai_events_inbox_{lane}.bad.jsonl",
        STATE_ROOT / f"ai_events_inbox_{lane}.cursor",
        STATE_ROOT / f"executor_{lane}.out.log",
        STATE_ROOT / f"executor_{lane}.err.log",
        STATE_ROOT / f"ws_switchboard_{lane}.out.log",
        STATE_ROOT / f"ws_switchboard_{lane}.err.log",
        STATE_ROOT / f"ws_switchboard_heartbeat_{lane}.txt",
        STATE_ROOT / "cursors" / f"observed_{lane}.cursor",
        STATE_ROOT / "offsets" / f"ai_action_router_{lane}.offset",
        STATE_ROOT / "ai_policy_sample" / f"{lane}.json",
        STATE_ROOT / f"ai_events_{lane}",
        GLOBAL_AI_EVENTS_DIR / lane,
    ]
    return paths


def _ledger_reset_payload(label: str, starting_equity: float | None, keep_equity: bool) -> Dict[str, Any]:
    path = PAPER_ROOT / f"{label}.json"
    current = _safe_read_json(path)
    loaded_start = current.get("starting_equity")
    loaded_equity = current.get("equity")
    try:
        baseline = float(starting_equity) if starting_equity is not None else float(loaded_start or 1000.0)
    except Exception:
        baseline = 1000.0
    try:
        equity = float(loaded_equity if keep_equity and loaded_equity is not None else baseline)
    except Exception:
        equity = baseline

    return {
        "account_label": label,
        "ai_profile": current.get("ai_profile"),
        "strategy_name": current.get("strategy_name"),
        "risk_pct": current.get("risk_pct", 0.0),
        "equity": float(equity),
        "starting_equity": float(equity if keep_equity else baseline),
        "created_ms": int(time.time() * 1000),
        "updated_ms": int(time.time() * 1000),
        "open_positions": [],
        "closed_trades": [],
    }


def _empty_positions_bus(label: str) -> Dict[str, Any]:
    return {
        "version": 2,
        "updated_ms": int(time.time() * 1000),
        "labels": {
            label: {
                "category": "linear",
                "positions": [],
            }
        },
    }


def _lane_ai_events_dir(label: str) -> Path:
    return STATE_ROOT / f"ai_events_{label}"


def _reset_lane_ai_events(label: str) -> List[Path]:
    lane_dir = _lane_ai_events_dir(label)
    lane_dir.mkdir(parents=True, exist_ok=True)
    rewritten: List[Path] = []
    empty_jsonl_names = [
        "executions.jsonl",
        "outcomes_orphans.jsonl",
        "outcomes_raw.jsonl",
        "outcomes.jsonl",
        "outcomes.v1.jsonl",
        "setups.jsonl",
        "spine_events.jsonl",
        "tp_sl_manager_events.jsonl",
        "ws_executions.jsonl",
    ]
    for name in empty_jsonl_names:
        target = lane_dir / name
        _atomic_write_text(target, "")
        rewritten.append(target)
    pending = lane_dir / "pending_setups.json"
    _atomic_write_json(pending, {})
    rewritten.append(pending)
    lock_path = lane_dir / "pending_setups.json.lock"
    if lock_path.exists():
        lock_path.unlink(missing_ok=True)  # type: ignore[arg-type]
    return rewritten


def _set_cursor_to_signal_tail(label: str) -> Tuple[Path, int]:
    signal_path = SIGNALS_ROOT / f"observed_{label}.jsonl"
    cursor_path = STATE_ROOT / "cursors" / f"observed_{label}.cursor"
    offset = 0
    try:
        if signal_path.exists():
            offset = int(signal_path.stat().st_size)
    except Exception:
        offset = 0
    _atomic_write_text(cursor_path, str(offset))
    return cursor_path, offset


def _write_router_offset(label: str, offset: int = 0) -> Path:
    path = STATE_ROOT / "offsets" / f"ai_action_router_{label}.offset"
    _atomic_write_text(path, str(int(max(0, offset))))
    return path


def _row_matches_label(row: Dict[str, Any], label: str) -> bool:
    lane = str(label or "").strip().lower()
    if not lane:
        return False
    acct = str(row.get("account_label") or row.get("label") or "").strip().lower()
    if acct == lane:
        return True
    trade_id = str(row.get("trade_id") or "").strip().lower()
    if trade_id.startswith(f"{lane}:") or trade_id.startswith(f"{lane}-"):
        return True
    return False


def _jsonl_line_matches_labels(raw: str, labels: List[str]) -> bool:
    line = str(raw or "").strip()
    if not line:
        return False
    try:
        obj = json.loads(line)
    except Exception:
        lower = line.lower()
        return any(
            f'"account_label": "{label}"' in lower
            or f'"trade_id": "{label}-' in lower
            or f'"trade_id": "{label}:' in lower
            for label in labels
        )
    if not isinstance(obj, dict):
        return False
    return any(_row_matches_label(obj, label) for label in labels)


def _rewrite_filtered_jsonl(path: Path, labels: List[str], archive_root: Path, apply: bool) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "path": str(path),
        "exists": path.exists(),
        "matched_rows": 0,
        "total_rows": 0,
        "applied": bool(apply),
    }
    if not path.exists():
        return result

    keep_lines: List[str] = []
    for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = str(raw or "")
        if not line.strip():
            continue
        result["total_rows"] = int(result["total_rows"]) + 1
        if _jsonl_line_matches_labels(line, labels):
            result["matched_rows"] = int(result["matched_rows"]) + 1
            continue
        keep_lines.append(line)

    if apply and int(result["matched_rows"]) > 0:
        _archive_path(path, archive_root)
        text = ("\n".join(keep_lines) + "\n") if keep_lines else ""
        _atomic_write_text(path, text)
        result["rewritten"] = True
    else:
        result["rewritten"] = False
    return result


def _reset_legacy_lane_outcomes(label: str, archive_root: Path, apply: bool) -> Dict[str, Any]:
    legacy_dir = GLOBAL_AI_EVENTS_DIR / str(label).strip().lower()
    result: Dict[str, Any] = {
        "label": label,
        "path": str(legacy_dir),
        "exists": legacy_dir.exists(),
        "files": [],
        "applied": bool(apply),
    }
    if not legacy_dir.exists():
        return result

    outcome_files = sorted(legacy_dir.glob("outcomes*.jsonl"))
    result["files"] = [str(p) for p in outcome_files]
    if apply and outcome_files:
        _archive_path(legacy_dir, archive_root)
        for path in outcome_files:
            _atomic_write_text(path, "")
    return result


def _clean_shared_outcomes(labels: List[str], tag: str, apply: bool) -> Dict[str, Any]:
    archive_root = STATE_ROOT / "runtime_resets" / tag / "_shared_outcomes"
    shared_rows = [
        _rewrite_filtered_jsonl(path, labels, archive_root, apply=apply)
        for path in SHARED_OUTCOME_FILES
    ]
    legacy_rows = [
        _reset_legacy_lane_outcomes(label, archive_root / "legacy_ai_events", apply=apply)
        for label in labels
    ]
    return {
        "archive_root": str(archive_root),
        "applied": bool(apply),
        "shared_files": shared_rows,
        "legacy_lane_dirs": legacy_rows,
    }


def _clean_lane(label: str, tag: str, *, starting_equity: float | None, keep_equity: bool, apply: bool) -> Dict[str, Any]:
    archive_root = _lane_archive_dir(tag, label)
    existing_paths = [path for path in _candidate_paths(label) if path.exists()]

    ledger_before = _safe_read_json(PAPER_ROOT / f"{label}.json")
    open_before = len(ledger_before.get("open_positions") or []) if isinstance(ledger_before.get("open_positions"), list) else 0
    closed_before = len(ledger_before.get("closed_trades") or []) if isinstance(ledger_before.get("closed_trades"), list) else 0

    summary: Dict[str, Any] = {
        "label": label,
        "archive_root": str(archive_root),
        "archived_count": len(existing_paths),
        "paper_open_before": open_before,
        "paper_closed_before": closed_before,
        "applied": bool(apply),
        "archived": [str(p) for p in existing_paths],
        "rewritten": [],
    }
    if not apply:
        return summary

    for src in existing_paths:
        _archive_path(src, archive_root)

    ledger_payload = _ledger_reset_payload(label, starting_equity=starting_equity, keep_equity=keep_equity)
    ledger_path = PAPER_ROOT / f"{label}.json"
    _atomic_write_json(ledger_path, ledger_payload)
    summary["rewritten"].append(str(ledger_path))

    positions_path = STATE_ROOT / f"positions_bus_{label}.json"
    _atomic_write_json(positions_path, _empty_positions_bus(label))
    summary["rewritten"].append(str(positions_path))

    for target in (
        STATE_ROOT / f"orderbook_bus_{label}.json",
        STATE_ROOT / f"trades_bus_{label}.json",
    ):
        _atomic_write_json(target, {"updated_ms": int(time.time() * 1000), "label": label})
        summary["rewritten"].append(str(target))

    for target in (
        STATE_ROOT / f"public_trades_{label}.jsonl",
        STATE_ROOT / f"ws_executions_{label}.jsonl",
        STATE_ROOT / f"ai_events_inbox_{label}.jsonl",
        STATE_ROOT / f"ai_events_inbox_{label}.bad.jsonl",
        STATE_ROOT / f"ai_decisions_{label}.jsonl",
    ):
        _atomic_write_text(target, "")
        summary["rewritten"].append(str(target))

    for target in (
        STATE_ROOT / f"trade_outcome_recorder_{label}.cursor",
        STATE_ROOT / f"ai_events_inbox_{label}.cursor",
    ):
        _atomic_write_text(target, "0")
        summary["rewritten"].append(str(target))

    ai_paths = _reset_lane_ai_events(label)
    summary["rewritten"].extend(str(p) for p in ai_paths)

    cursor_path, signal_offset = _set_cursor_to_signal_tail(label)
    summary["rewritten"].append(str(cursor_path))
    summary["observed_signal_tail_offset"] = signal_offset

    router_path = _write_router_offset(label, 0)
    summary["rewritten"].append(str(router_path))

    return summary


def build_reset_report(
    *,
    labels: Iterable[str],
    tag: str,
    starting_equity: float | None,
    keep_equity: bool,
    apply: bool,
) -> Dict[str, Any]:
    label_list = [str(label).strip().lower() for label in labels if str(label).strip()]
    rows = [
        _clean_lane(
            label,
            tag,
            starting_equity=starting_equity,
            keep_equity=keep_equity,
            apply=apply,
        )
        for label in label_list
    ]
    shared_cleanup = _clean_shared_outcomes(label_list, tag, apply=apply)
    return {
        "schema_version": "runtime.reset.v1",
        "generated_ms": int(time.time() * 1000),
        "archive_tag": tag,
        "apply": bool(apply),
        "keep_equity": bool(keep_equity),
        "starting_equity_override": starting_equity,
        "labels": rows,
        "label_count": len(rows),
        "shared_outcomes_cleanup": shared_cleanup,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Archive and reset contaminated lane runtime state for a fresh paper sample.")
    ap.add_argument("--labels", nargs="*", default=DEFAULT_LABELS, help="lane labels to reset (default: flashback01..flashback09)")
    ap.add_argument("--include-disabled", action="store_true", help="include disabled labels from subaccounts.yaml")
    ap.add_argument("--archive-tag", default="", help="optional archive tag suffix")
    ap.add_argument("--starting-equity", type=float, default=None, help="optional equity baseline override for reset ledgers")
    ap.add_argument("--keep-equity", action="store_true", help="carry current paper equity into the new clean baseline")
    ap.add_argument("--apply", action="store_true", help="write the reset; default is dry-run")
    args = ap.parse_args()

    labels = _selected_labels(args.labels, include_disabled=bool(args.include_disabled))
    if not labels:
        raise SystemExit("No labels selected for runtime reset.")

    tag = _now_tag()
    if args.archive_tag:
        tag = f"{tag}_{str(args.archive_tag).strip()}"

    report = build_reset_report(
        labels=labels,
        tag=tag,
        starting_equity=args.starting_equity,
        keep_equity=bool(args.keep_equity),
        apply=bool(args.apply),
    )

    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
