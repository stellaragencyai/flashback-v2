#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import yaml  # type: ignore
except Exception as exc:  # pragma: no cover
    yaml = None  # type: ignore
    YAML_IMPORT_ERROR = str(exc)
else:
    YAML_IMPORT_ERROR = None

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.core.notifier_bot import _resolve_tg_creds, tg_send
try:
    from app.tools.fleet_meta_brain import write_meta_outputs  # type: ignore
except Exception:  # pragma: no cover
    write_meta_outputs = None  # type: ignore

OPERATOR_STATE_PATH = REPO_ROOT / "state" / "fleet_operator_state.json"
MAIN_UTILS_STATUS_PATH = REPO_ROOT / "state" / "main_account" / "supervisor_status.json"

STRATEGIES_PATH = REPO_ROOT / "config" / "strategies.yaml"
SUBACCOUNTS_PATH = REPO_ROOT / "config" / "subaccounts.yaml"
FLEET_MANIFEST_PATH = REPO_ROOT / "config" / "fleet_manifest.yaml"
DEFAULT_WINDOW = "all"


def _safe_read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8", errors="ignore"))
    except Exception:
        return {}


def _atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.tmp")
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp.replace(path)


def _safe_read_yaml(path: Path) -> Dict[str, Any]:
    if yaml is None:
        raise RuntimeError(f"PyYAML unavailable: {YAML_IMPORT_ERROR}")
    if not path.exists():
        raise FileNotFoundError(str(path))
    data = yaml.safe_load(path.read_text(encoding="utf-8", errors="ignore")) or {}
    if not isinstance(data, dict):
        raise RuntimeError(f"{path.name} root must be a mapping")
    return data


def _atomic_write_yaml(path: Path, payload: Dict[str, Any]) -> None:
    if yaml is None:
        raise RuntimeError(f"PyYAML unavailable: {YAML_IMPORT_ERROR}")
    tmp = path.with_name(f"{path.name}.tmp")
    tmp.write_text(yaml.safe_dump(payload, sort_keys=False, default_flow_style=False), encoding="utf-8")
    tmp.replace(path)


def _is_account_mapping(label: str, node: Any) -> bool:
    return label not in {"version", "notes", "legacy"} and isinstance(node, dict)


def _sync_subaccounts_legacy(cfg: Dict[str, Any]) -> None:
    legacy = cfg.get("legacy")
    if not isinstance(legacy, dict):
        legacy = {}
        cfg["legacy"] = legacy
    rows: List[Dict[str, Any]] = []
    for key, value in cfg.items():
        if not _is_account_mapping(str(key), value):
            continue
        row = dict(value)
        row["account_label"] = str(key)
        rows.append(row)
    legacy["accounts_list"] = rows


def _find_strategy_row(strat_cfg: Dict[str, Any], label: str) -> Dict[str, Any]:
    rows = strat_cfg.get("subaccounts")
    if not isinstance(rows, list):
        raise RuntimeError("strategies.yaml missing top-level subaccounts list")
    for row in rows:
        if isinstance(row, dict) and str(row.get("account_label") or "").strip() == label:
            return row
    raise KeyError(f"Unknown label in strategies.yaml: {label}")


def _find_subaccount_rows(sub_cfg: Dict[str, Any], label: str) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
    top = sub_cfg.get(label)
    if not isinstance(top, dict):
        raise KeyError(f"Unknown label in subaccounts.yaml: {label}")
    legacy = sub_cfg.get("legacy")
    legacy_row = None
    if isinstance(legacy, dict):
        rows = legacy.get("accounts_list")
        if isinstance(rows, list):
            for row in rows:
                if isinstance(row, dict) and str(row.get("account_label") or "").strip() == label:
                    legacy_row = row
                    break
    return top, legacy_row


def _load_operator_state() -> Dict[str, Any]:
    state = _safe_read_json(OPERATOR_STATE_PATH)
    if not state:
        state = {
            "schema_version": "fleet.operator.state.v1",
            "last_update_id": 0,
            "baseline": {},
            "paused": {},
            "approval_decisions": {},
            "history": [],
        }
    for key in ("baseline", "paused", "approval_decisions"):
        if not isinstance(state.get(key), dict):
            state[key] = {}
    if not isinstance(state.get("history"), list):
        state["history"] = []
    return state


def _save_operator_state(state: Dict[str, Any]) -> None:
    state["schema_version"] = "fleet.operator.state.v1"
    state["updated_ms"] = int(time.time() * 1000)
    _atomic_write_json(OPERATOR_STATE_PATH, state)


def _snapshot_current(label: str, strat_cfg: Dict[str, Any], sub_cfg: Dict[str, Any]) -> Dict[str, Any]:
    strategy_row = dict(_find_strategy_row(strat_cfg, label))
    top_row, _legacy_row = _find_subaccount_rows(sub_cfg, label)
    return {
        "strategies": {
            "enabled": bool(strategy_row.get("enabled", True)),
            "automation_mode": strategy_row.get("automation_mode"),
            "risk_pct": strategy_row.get("risk_pct"),
        },
        "subaccounts": {
            "enabled": bool(top_row.get("enabled", True)),
            "automation_mode": top_row.get("automation_mode"),
        },
    }


def sync_config_truth() -> Dict[str, Any]:
    strat_cfg = _safe_read_yaml(STRATEGIES_PATH)
    sub_cfg = _safe_read_yaml(SUBACCOUNTS_PATH)
    _sync_subaccounts_legacy(sub_cfg)
    _atomic_write_yaml(SUBACCOUNTS_PATH, sub_cfg)

    from app.tools.build_fleet_manifest import build_manifest

    manifest = build_manifest()
    FLEET_MANIFEST_PATH.write_text(
        "# config/fleet_manifest.yaml\n"
        "#\n"
        "# GENERATED FILE — DO NOT EDIT BY HAND.\n"
        "# Built by: app/tools/build_fleet_manifest.py\n"
        "# Source of truth: config/strategies.yaml + config/subaccounts.yaml\n"
        "#\n"
        + yaml.safe_dump(manifest, sort_keys=False, default_flow_style=False),
        encoding="utf-8",
    )
    return {
        "strategy_rows": len(strat_cfg.get("subaccounts") or []),
        "subaccount_rows": len([k for k, v in sub_cfg.items() if _is_account_mapping(str(k), v)]),
    }


def _ensure_baseline(label: str, state: Dict[str, Any], strat_cfg: Dict[str, Any], sub_cfg: Dict[str, Any]) -> None:
    baseline = state.setdefault("baseline", {})
    if label not in baseline:
        baseline[label] = _snapshot_current(label, strat_cfg, sub_cfg)


def _set_lane_runtime(
    label: str,
    *,
    automation_mode: str,
    enabled: bool,
    risk_pct: Optional[float],
    state: Dict[str, Any],
) -> Dict[str, Any]:
    strat_cfg = _safe_read_yaml(STRATEGIES_PATH)
    sub_cfg = _safe_read_yaml(SUBACCOUNTS_PATH)
    _ensure_baseline(label, state, strat_cfg, sub_cfg)
    strategy_row = _find_strategy_row(strat_cfg, label)
    top_row, legacy_row = _find_subaccount_rows(sub_cfg, label)

    strategy_row["enabled"] = bool(enabled)
    strategy_row["automation_mode"] = str(automation_mode).strip().upper()
    if risk_pct is not None:
        strategy_row["risk_pct"] = float(risk_pct)

    top_row["enabled"] = bool(enabled)
    top_row["automation_mode"] = str(automation_mode).strip().upper()
    if legacy_row is not None:
        legacy_row["enabled"] = bool(enabled)
        legacy_row["automation_mode"] = str(automation_mode).strip().upper()

    _atomic_write_yaml(STRATEGIES_PATH, strat_cfg)
    sync_config_truth()
    return _snapshot_current(label, strat_cfg, sub_cfg)


def _try_restart_stack() -> Tuple[bool, str]:
    return _run_service_action("restart", "flashback-stack.service")


def _run_service_action(action: str, service: str) -> Tuple[bool, str]:
    try:
        result = subprocess.run(
            ["sudo", "-n", "systemctl", action, service],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=20,
        )
    except Exception as exc:
        return False, f"restart exception: {exc}"
    if result.returncode == 0:
        if action == "is-active":
            detail = (result.stdout or "").strip() or "unknown"
            return detail == "active", detail
        return True, f"{service} {action}ed"
    detail = (result.stderr or result.stdout or "").strip()
    return False, detail or f"restart failed rc={result.returncode}"


def _refresh_payload(window: str = DEFAULT_WINDOW) -> Dict[str, Any]:
    if write_meta_outputs is None:
        return {}
    return write_meta_outputs(window=window)


def _queue_map(payload: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for row in payload.get("approval_queue", []) or []:
        if isinstance(row, dict):
            label = str(row.get("label") or "").strip()
            if label:
                out[label] = row
    return out


def _rebuild_map(payload: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    rebuild_plan = payload.get("rebuild_plan")
    targets = rebuild_plan.get("targets") if isinstance(rebuild_plan, dict) else None
    if isinstance(targets, list):
        for row in targets:
            if isinstance(row, dict):
                label = str(row.get("target_label") or "").strip()
                if label:
                    out[label] = row
    return out


def _approval_signature_for(label: str, queue_map: Dict[str, Dict[str, Any]]) -> str:
    item = queue_map.get(label) or {}
    return str(item.get("approval_signature") or "").strip()


def _append_history(state: Dict[str, Any], action: str, label: Optional[str], detail: str) -> None:
    history = state.setdefault("history", [])
    history.append(
        {
            "ts_ms": int(time.time() * 1000),
            "action": action,
            "label": label,
            "detail": detail,
        }
    )
    if len(history) > 200:
        del history[:-200]


def _mode_from_hint(queue_mode: Optional[str], hint: Optional[str]) -> str:
    hint_norm = str(hint or "").strip().upper()
    if hint_norm in {"FULL", "LIVE"}:
        return "LIVE"
    if hint_norm in {"CANARY", "PILOT"}:
        return "LIVE_CANARY"
    if hint_norm in {"DRY", "PAPER"}:
        return "LEARN_DRY"
    if queue_mode in {"pilot_live", "scale_live", "shadow_live"}:
        return "LIVE_CANARY"
    return "LEARN_DRY"


def _canonical_mode(mode_hint: Optional[str]) -> str:
    hint_norm = str(mode_hint or "").strip().upper()
    if hint_norm in {"LIVE", "REAL", "FULL", "LIVE_FULL"}:
        return "LIVE_FULL"
    if hint_norm in {"CANARY", "PILOT", "LIVE_CANARY"}:
        return "LIVE_CANARY"
    if hint_norm in {"DRY", "PAPER", "LEARN_DRY"}:
        return "LEARN_DRY"
    if hint_norm == "OFF":
        return "OFF"
    return "LEARN_DRY"


def _approve_lane(label: str, hint: Optional[str], state: Dict[str, Any], restart_stack: bool) -> str:
    payload = _refresh_payload()
    queue = _queue_map(payload)
    item = queue.get(label)
    if item is None:
        return f"{label}: not currently in approval queue."

    strat_cfg = _safe_read_yaml(STRATEGIES_PATH)
    sub_cfg = _safe_read_yaml(SUBACCOUNTS_PATH)
    _ensure_baseline(label, state, strat_cfg, sub_cfg)
    target_mode = _mode_from_hint(str(item.get("approval_mode") or ""), hint)
    risk_pct = _safe_float(item.get("recommended_risk_pct"))
    if risk_pct is None:
        risk_pct = _safe_float((state.get("baseline", {}).get(label, {}).get("strategies", {}) or {}).get("risk_pct"))
    updated = _set_lane_runtime(label, automation_mode=target_mode, enabled=True, risk_pct=risk_pct, state=state)
    state["approval_decisions"][label] = {
        "decision": f"approved:{target_mode.lower()}",
        "signature": _approval_signature_for(label, queue),
        "applied_ms": int(time.time() * 1000),
        "recommended_risk_pct": risk_pct,
    }
    state.get("paused", {}).pop(label, None)
    restart_ok, restart_detail = _try_restart_stack() if restart_stack else (False, "restart skipped")
    _append_history(
        state,
        "approve",
        label,
        f"mode={target_mode} risk_pct={updated['strategies'].get('risk_pct')} restart={restart_ok}",
    )
    _save_operator_state(state)
    _refresh_payload()
    return (
        f"APPROVED {label}\n"
        f"Mode: {target_mode}\n"
        f"Risk: {float(risk_pct or 0.0) * 100.0:.3f}%\n"
        f"Restart: {'OK' if restart_ok else restart_detail}"
    )


def _reject_lane(label: str, state: Dict[str, Any], restart_stack: bool) -> str:
    strat_cfg = _safe_read_yaml(STRATEGIES_PATH)
    sub_cfg = _safe_read_yaml(SUBACCOUNTS_PATH)
    _ensure_baseline(label, state, strat_cfg, sub_cfg)
    payload = _refresh_payload()
    queue = _queue_map(payload)
    baseline = state["baseline"].get(label, {})
    base_risk = baseline.get("strategies", {}).get("risk_pct")
    base_mode = baseline.get("strategies", {}).get("automation_mode") or "LEARN_DRY"
    _set_lane_runtime(label, automation_mode=str(base_mode), enabled=True, risk_pct=_safe_float(base_risk), state=state)
    state["approval_decisions"][label] = {
        "decision": "rejected",
        "signature": _approval_signature_for(label, queue),
        "applied_ms": int(time.time() * 1000),
    }
    restart_ok, restart_detail = _try_restart_stack() if restart_stack else (False, "restart skipped")
    _append_history(state, "reject", label, f"mode={base_mode} restart={restart_ok}")
    _save_operator_state(state)
    _refresh_payload()
    return (
        f"REJECTED {label}\n"
        f"Mode: {base_mode}\n"
        f"Risk: {float(_safe_float(base_risk) or 0.0) * 100.0:.3f}%\n"
        f"Restart: {'OK' if restart_ok else restart_detail}"
    )


def _pause_lane(label: str, state: Dict[str, Any], restart_stack: bool) -> str:
    strat_cfg = _safe_read_yaml(STRATEGIES_PATH)
    sub_cfg = _safe_read_yaml(SUBACCOUNTS_PATH)
    state.setdefault("paused", {})[label] = _snapshot_current(label, strat_cfg, sub_cfg)
    _set_lane_runtime(label, automation_mode="OFF", enabled=False, risk_pct=None, state=state)
    restart_ok, restart_detail = _try_restart_stack() if restart_stack else (False, "restart skipped")
    _append_history(state, "pause", label, f"restart={restart_ok}")
    _save_operator_state(state)
    _refresh_payload()
    return f"PAUSED {label}\nMode: OFF\nRestart: {'OK' if restart_ok else restart_detail}"


def _resume_lane(label: str, state: Dict[str, Any], restart_stack: bool) -> str:
    paused = state.setdefault("paused", {}).pop(label, None)
    if not isinstance(paused, dict):
        baseline = state.setdefault("baseline", {}).get(label, {})
        paused = baseline
    target_mode = str((paused.get("strategies") or {}).get("automation_mode") or "LEARN_DRY")
    target_enabled = bool((paused.get("strategies") or {}).get("enabled", True))
    target_risk = _safe_float((paused.get("strategies") or {}).get("risk_pct"))
    _set_lane_runtime(label, automation_mode=target_mode, enabled=target_enabled, risk_pct=target_risk, state=state)
    restart_ok, restart_detail = _try_restart_stack() if restart_stack else (False, "restart skipped")
    _append_history(state, "resume", label, f"mode={target_mode} restart={restart_ok}")
    _save_operator_state(state)
    _refresh_payload()
    return (
        f"RESUMED {label}\n"
        f"Mode: {target_mode}\n"
        f"Risk: {float(target_risk or 0.0) * 100.0:.3f}%\n"
        f"Restart: {'OK' if restart_ok else restart_detail}"
    )


def _force_lane_mode(label: str, mode_hint: str, state: Dict[str, Any], restart_stack: bool) -> str:
    strat_cfg = _safe_read_yaml(STRATEGIES_PATH)
    sub_cfg = _safe_read_yaml(SUBACCOUNTS_PATH)
    _ensure_baseline(label, state, strat_cfg, sub_cfg)

    target_mode = _canonical_mode(mode_hint)
    baseline = state.get("baseline", {}).get(label, {})
    current = _snapshot_current(label, strat_cfg, sub_cfg)
    target_risk = _safe_float((current.get("strategies") or {}).get("risk_pct"))
    if target_risk is None:
        target_risk = _safe_float((baseline.get("strategies") or {}).get("risk_pct"))

    enabled = target_mode != "OFF"
    updated = _set_lane_runtime(label, automation_mode=target_mode, enabled=enabled, risk_pct=target_risk, state=state)
    if target_mode == "OFF":
        state.setdefault("paused", {})[label] = current
    else:
        state.setdefault("paused", {}).pop(label, None)
    restart_ok, restart_detail = _try_restart_stack() if restart_stack else (False, "restart skipped")
    _append_history(state, "set_mode", label, f"mode={target_mode} restart={restart_ok}")
    _save_operator_state(state)
    _refresh_payload()
    return (
        f"MODE {label}\n"
        f"Mode: {target_mode}\n"
        f"Enabled: {'YES' if updated['strategies'].get('enabled') else 'NO'}\n"
        f"Risk: {float(target_risk or 0.0) * 100.0:.3f}%\n"
        f"Restart: {'OK' if restart_ok else restart_detail}"
    )


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, "", "null"):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _queue_text(payload: Dict[str, Any]) -> str:
    queue = payload.get("approval_queue", []) or []
    if not queue:
        return "QUEUE\nNo pending approvals."
    lines = ["QUEUE"]
    for item in queue[:8]:
        lines.append(
            f"{item.get('label')} | {item.get('approval_mode')} | "
            f"rank={float(item.get('executive_rank_score') or 0.0) * 100.0:.1f}% | "
            f"risk={float(item.get('recommended_risk_pct') or 0.0) * 100.0:.3f}%"
        )
    return "\n".join(lines)


def _status_text(payload: Dict[str, Any]) -> str:
    summary = payload.get("portfolio_summary", {}) or {}
    top = summary.get("top_ranked_labels", []) or []
    return (
        "STATUS\n"
        f"Heat: {float(summary.get('portfolio_heat') or 0.0) * 100.0:.3f}% / "
        f"{float(summary.get('portfolio_heat_cap_pct') or 0.0) * 100.0:.1f}% cap\n"
        f"Kill-fast: {int(summary.get('kill_fast_count') or 0)}\n"
        f"Pending approvals: {int(summary.get('approval_required_count') or 0)}\n"
        f"Rebuild targets: {int((payload.get('rebuild_plan') or {}).get('rebuild_count') or 0)}\n"
        f"Top lanes: {', '.join(str(x) for x in top) if top else 'none'}"
    )


def _rebuild_text(payload: Dict[str, Any], label: Optional[str]) -> str:
    rebuilds = _rebuild_map(payload)
    if label:
        row = rebuilds.get(label)
        if row is None:
            return f"REBUILD {label}\nNo rebuild plan queued."
        return (
            f"REBUILD {label}\n"
            f"Blueprint: {row.get('blueprint')}\n"
            f"Donor: {row.get('donor_label')} ({row.get('donor_strategy_name')})\n"
            f"Target: {row.get('target_strategy_name')}\n"
            f"Score: {float(row.get('score') or 0.0):.4f}"
        )
    if not rebuilds:
        return "REBUILD\nNo rebuild targets."
    lines = ["REBUILD"]
    for row in list(rebuilds.values())[:8]:
        lines.append(
            f"{row.get('target_label')} <= {row.get('donor_label')} | "
            f"{row.get('blueprint')} | score={float(row.get('score') or 0.0):.4f}"
        )
    return "\n".join(lines)


def _help_text() -> str:
    return (
        "FLASHBACK OPS\n"
        "APPROVE flashback08\n"
        "APPROVE flashback09 FULL\n"
        "CANARY flashback08\n"
        "LIVE flashback08\n"
        "DRY flashback08\n"
        "REJECT flashback08\n"
        "PAUSE flashback04\n"
        "RESUME flashback04\n"
        "MAINUTILS STATUS\n"
        "MAINUTILS RESTART\n"
        "SYNC\n"
        "QUEUE\n"
        "STATUS\n"
        "REBUILD\n"
        "REBUILD flashback04"
    )


def _main_utils_status_text() -> str:
    status = _safe_read_json(MAIN_UTILS_STATUS_PATH)
    if not status:
        active_ok, active_detail = _run_service_action("is-active", "flashback-main-account.service")
        return (
            "MAINUTILS\n"
            f"Service: {'active' if active_ok else active_detail}\n"
            "No supervisor_status.json yet."
        )

    workers = status.get("workers") if isinstance(status.get("workers"), dict) else {}
    worker_lines: List[str] = []
    for name, row in workers.items():
        if not isinstance(row, dict):
            continue
        worker_lines.append(
            f"{name}={'UP' if row.get('alive') else 'DOWN'} restart_count={int(row.get('restart_count', 0) or 0)}"
        )
    active_ok, active_detail = _run_service_action("is-active", "flashback-main-account.service")
    return (
        "MAINUTILS\n"
        f"Service: {'active' if active_ok else active_detail}\n"
        f"Workers: {int(status.get('running_workers', 0) or 0)}/{int(status.get('enabled_workers', 0) or 0)}\n"
        f"All running: {'YES' if status.get('all_running') else 'NO'}\n"
        + ("\n".join(worker_lines) if worker_lines else "No workers reported.")
    )


def _main_utils_command(action: Optional[str]) -> str:
    act = str(action or "STATUS").strip().upper()
    if act in {"STATUS", "SHOW"}:
        return _main_utils_status_text()
    if act == "START":
        ok, detail = _run_service_action("start", "flashback-main-account.service")
        return f"MAINUTILS START\n{detail if ok else 'ERROR: ' + detail}"
    if act == "STOP":
        ok, detail = _run_service_action("stop", "flashback-main-account.service")
        return f"MAINUTILS STOP\n{detail if ok else 'ERROR: ' + detail}"
    if act == "RESTART":
        ok, detail = _run_service_action("restart", "flashback-main-account.service")
        return f"MAINUTILS RESTART\n{detail if ok else 'ERROR: ' + detail}"
    return _help_text()


def _handle_command(text: str, state: Dict[str, Any], restart_stack: bool) -> str:
    parts = [p for p in str(text or "").strip().split() if p]
    if not parts:
        return ""
    verb = parts[0].lstrip("/").upper()
    label = parts[1].strip().lower() if len(parts) >= 2 else None
    extra = parts[2].strip() if len(parts) >= 3 else None
    payload = _refresh_payload()

    if verb == "HELP":
        return _help_text()
    if verb == "QUEUE":
        return _queue_text(payload)
    if verb == "STATUS":
        return _status_text(payload)
    if verb == "SYNC":
        info = sync_config_truth()
        return (
            "SYNC\n"
            f"Manifest refreshed.\n"
            f"strategies={int(info.get('strategy_rows') or 0)} "
            f"subaccounts={int(info.get('subaccount_rows') or 0)}"
        )
    if verb in {"MAINUTILS", "MAIN-UTILS"}:
        return _main_utils_command(label)
    if verb == "REBUILD":
        return _rebuild_text(payload, label)
    if verb == "APPROVE" and label:
        return _approve_lane(label, extra, state, restart_stack)
    if verb in {"CANARY", "LIVE", "REAL", "DRY"} and label:
        return _force_lane_mode(label, verb, state, restart_stack)
    if verb == "REJECT" and label:
        return _reject_lane(label, state, restart_stack)
    if verb == "PAUSE" and label:
        return _pause_lane(label, state, restart_stack)
    if verb == "RESUME" and label:
        return _resume_lane(label, state, restart_stack)
    return _help_text()


def apply_text_command(text: str, restart_stack: bool = False) -> Dict[str, Any]:
    state = _load_operator_state()
    reply = _handle_command(text, state, restart_stack)
    _save_operator_state(state)
    return {
        "ok": bool(reply),
        "reply": reply or _help_text(),
    }


def _telegram_get_updates(token: str, offset: int, timeout_sec: int) -> List[Dict[str, Any]]:
    params = {
        "timeout": str(max(0, timeout_sec)),
        "offset": str(offset),
        "allowed_updates": json.dumps(["message"]),
    }
    url = f"https://api.telegram.org/bot{token}/getUpdates?{urllib.parse.urlencode(params)}"
    with urllib.request.urlopen(url, timeout=timeout_sec + 5) as resp:
        payload = json.loads(resp.read().decode("utf-8", errors="ignore"))
    if not isinstance(payload, dict) or payload.get("ok") is not True:
        return []
    result = payload.get("result")
    return result if isinstance(result, list) else []


def run_once(timeout_sec: int = 2, restart_stack: bool = False, send_replies: bool = True) -> Dict[str, Any]:
    token, chat_id, source = _resolve_tg_creds("main")
    if not token or not chat_id:
        raise RuntimeError(f"main telegram creds missing ({source})")
    state = _load_operator_state()
    updates = _telegram_get_updates(token, int(state.get("last_update_id", 0) or 0) + 1, timeout_sec)
    handled: List[Dict[str, Any]] = []
    expected_chat_id = str(chat_id).strip()

    for item in updates:
        if not isinstance(item, dict):
            continue
        update_id = int(item.get("update_id") or 0)
        if update_id > int(state.get("last_update_id", 0) or 0):
            state["last_update_id"] = update_id
        message = item.get("message")
        if not isinstance(message, dict):
            continue
        incoming_chat = str(((message.get("chat") or {}).get("id")) or "").strip()
        if expected_chat_id and incoming_chat != expected_chat_id:
            continue
        text = str(message.get("text") or "").strip()
        if not text:
            continue
        reply = _handle_command(text, state, restart_stack=restart_stack)
        handled.append({"text": text, "reply": reply})
        if send_replies and reply:
            tg_send(reply, channel="main", level="info")

    _save_operator_state(state)
    return {
        "schema_version": "fleet.operator.run.v1",
        "handled_count": len(handled),
        "handled": handled,
        "last_update_id": state.get("last_update_id", 0),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Flashback Telegram operator command plane")
    ap.add_argument("--watch", action="store_true", help="loop forever")
    ap.add_argument("--poll-sec", type=int, default=10)
    ap.add_argument("--timeout-sec", type=int, default=2)
    ap.add_argument("--restart-stack", action="store_true")
    ap.add_argument("--no-reply", action="store_true")
    args = ap.parse_args()

    if args.watch:
        while True:
            result = run_once(timeout_sec=args.timeout_sec, restart_stack=args.restart_stack, send_replies=not args.no_reply)
            print(json.dumps(result, indent=2))
            time.sleep(max(1, args.poll_sec))
    else:
        result = run_once(timeout_sec=args.timeout_sec, restart_stack=args.restart_stack, send_replies=not args.no_reply)
        print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
