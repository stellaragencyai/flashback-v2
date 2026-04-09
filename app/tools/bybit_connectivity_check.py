#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests


THIS_FILE = Path(__file__).resolve()
REPO_ROOT = THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.core.flashback_common import _headers  # type: ignore


SUBACCOUNTS_PATH = REPO_ROOT / "config" / "subaccounts.yaml"
ENV_DEFAULT = REPO_ROOT / "deploy" / "flashback_pi.env"
BYBIT_BASE = os.getenv("BYBIT_BASE", "https://api.bybit.com").rstrip("/")
DEFAULT_LABELS = [f"flashback{idx:02d}" for idx in range(1, 11)]
PUBLIC_TIME_PATH = "/v5/market/time"

try:
    import yaml  # type: ignore
except Exception:
    yaml = None  # type: ignore


def _safe_read_yaml(path: Path) -> Dict[str, Any]:
    if yaml is None or not path.exists():
        return {}
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8", errors="ignore")) or {}
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _load_env_file(path: Path) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not path.exists():
        return out
    for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = str(raw or "").strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        out[str(key).strip()] = str(value).strip().strip("'").strip('"')
    return out


def _enabled_map() -> Dict[str, bool]:
    cfg = _safe_read_yaml(SUBACCOUNTS_PATH)
    out: Dict[str, bool] = {"main": True}
    for label, node in cfg.items():
        if label in {"version", "notes", "legacy"} or not isinstance(node, dict):
            continue
        out[str(label)] = bool(node.get("enabled", True))
    return out


def _selected_labels(raw_labels: Iterable[str], include_main: bool, include_disabled: bool) -> List[str]:
    enabled_map = _enabled_map()
    labels = [str(v).strip().lower() for v in raw_labels if str(v).strip()]
    if not labels:
        labels = list(DEFAULT_LABELS)
        if include_main:
            labels.insert(0, "main")
    elif include_main and "main" not in labels:
        labels.insert(0, "main")
    out: List[str] = []
    for label in labels:
        if not include_disabled and label in enabled_map and not enabled_map[label]:
            continue
        out.append(label)
    return sorted(list(dict.fromkeys(out)))


def _pair_from_env(env_map: Dict[str, str], *names: str) -> Tuple[str, str, str]:
    for idx in range(0, len(names), 2):
        key_name = names[idx]
        sec_name = names[idx + 1]
        key = str(env_map.get(key_name) or os.getenv(key_name) or "").strip()
        sec = str(env_map.get(sec_name) or os.getenv(sec_name) or "").strip()
        if key and sec:
            return key, sec, f"{key_name}/{sec_name}"
    return "", "", ""


def _resolve_label_creds(label: str, env_map: Dict[str, str]) -> Dict[str, Any]:
    label_upper = str(label or "").strip().upper()
    if str(label).strip().lower() == "main":
        read_key, read_secret, read_source = _pair_from_env(
            env_map,
            "BYBIT_MAIN_READ_KEY", "BYBIT_MAIN_READ_SECRET",
            "BYBIT_MAIN_API_KEY", "BYBIT_MAIN_API_SECRET",
            "BYBIT_API_KEY", "BYBIT_API_SECRET",
        )
        ws_key, ws_secret, ws_source = _pair_from_env(
            env_map,
            "BYBIT_MAIN_WEBSOCKET_KEY", "BYBIT_MAIN_WEBSOCKET_SECRET",
            "BYBIT_MAIN_API_KEY", "BYBIT_MAIN_API_SECRET",
            "BYBIT_API_KEY", "BYBIT_API_SECRET",
        )
    else:
        read_key, read_secret, read_source = _pair_from_env(
            env_map,
            f"BYBIT_{label_upper}_READ_KEY", f"BYBIT_{label_upper}_READ_SECRET",
            f"BYBIT_{label_upper}_API_KEY", f"BYBIT_{label_upper}_API_SECRET",
        )
        ws_key, ws_secret, ws_source = _pair_from_env(
            env_map,
            f"BYBIT_{label_upper}_API_KEY", f"BYBIT_{label_upper}_API_SECRET",
        )

    return {
        "read_key": read_key,
        "read_secret": read_secret,
        "read_source": read_source,
        "ws_key": ws_key,
        "ws_secret": ws_secret,
        "ws_source": ws_source,
    }


def _body_preview(text: str, limit: int = 240) -> str:
    preview = " ".join(str(text or "").split())
    return preview[:limit]


def _diagnose_http(status_code: int, content_type: str, body_preview: str) -> str:
    body = str(body_preview or "").lower()
    ctype = str(content_type or "").lower()
    if status_code == 403 and "text/html" in ctype:
        if "request could not be satisfied" in body:
            return "blocked_html_403"
        return "html_403"
    if status_code == 401:
        return "auth_http_401"
    if status_code == 429:
        return "rate_limited"
    if "application/json" not in ctype:
        return "non_json_response"
    return "unknown_http_failure"


def _public_time_probe(timeout_sec: float) -> Dict[str, Any]:
    url = f"{BYBIT_BASE}{PUBLIC_TIME_PATH}"
    started = time.time()
    try:
        resp = requests.get(url, timeout=timeout_sec)
    except Exception as exc:
        return {
            "ok": False,
            "url": url,
            "status_code": None,
            "content_type": None,
            "elapsed_ms": int((time.time() - started) * 1000),
            "body_preview": None,
            "diagnosis": "public_probe_request_failed",
            "error_detail": f"{type(exc).__name__}: {exc}",
        }

    elapsed_ms = int((time.time() - started) * 1000)
    content_type = str(resp.headers.get("content-type") or "").strip()
    body_preview = _body_preview(resp.text)
    result: Dict[str, Any] = {
        "ok": False,
        "url": url,
        "status_code": int(resp.status_code),
        "content_type": content_type,
        "elapsed_ms": elapsed_ms,
        "body_preview": body_preview,
        "diagnosis": None,
        "error_detail": None,
    }
    try:
        payload = resp.json()
    except Exception:
        result["diagnosis"] = _diagnose_http(resp.status_code, content_type, body_preview)
        return result

    ret_code = int(payload.get("retCode", -1))
    result["ret_code"] = ret_code
    result["ret_msg"] = str(payload.get("retMsg") or "").strip()
    result["time_now"] = ((payload.get("result") or {}) if isinstance(payload.get("result"), dict) else {}).get("timeSecond")
    result["ok"] = resp.status_code == 200 and ret_code == 0
    if not result["ok"]:
        result["diagnosis"] = f"json_retcode_{ret_code}"
    return result


def _wallet_balance_probe(key: str, secret: str, timeout_sec: float) -> Dict[str, Any]:
    query = "accountType=UNIFIED"
    url = f"{BYBIT_BASE}/v5/account/wallet-balance?{query}"
    headers = _headers(key, secret, query=query)
    started = time.time()
    resp = requests.get(url, headers=headers, timeout=timeout_sec)
    elapsed_ms = int((time.time() - started) * 1000)
    content_type = str(resp.headers.get("content-type") or "").strip()
    body_preview = _body_preview(resp.text)
    try:
        payload = resp.json()
    except Exception as exc:
        return {
            "ok": False,
            "status_code": int(resp.status_code),
            "ret_code": None,
            "ret_msg": None,
            "elapsed_ms": elapsed_ms,
            "equity_usdt": None,
            "content_type": content_type,
            "body_preview": body_preview,
            "diagnosis": _diagnose_http(resp.status_code, content_type, body_preview),
            "error_detail": f"{type(exc).__name__}: {exc}",
        }

    ret_code = int(payload.get("retCode", -1))
    ok = resp.status_code == 200 and ret_code == 0
    equity_usdt = None
    if ok:
        try:
            total = 0.0
            for account in payload.get("result", {}).get("list", []) or []:
                for coin in account.get("coin", []) or []:
                    if str(coin.get("coin") or "").upper() != "USDT":
                        continue
                    total += float(coin.get("equity") or 0.0)
            equity_usdt = round(total, 8)
        except Exception:
            equity_usdt = None
    return {
        "ok": ok,
        "status_code": int(resp.status_code),
        "ret_code": ret_code,
        "ret_msg": str(payload.get("retMsg") or "").strip(),
        "elapsed_ms": elapsed_ms,
        "equity_usdt": equity_usdt,
        "content_type": content_type,
        "body_preview": body_preview,
        "diagnosis": None if ok else f"json_retcode_{ret_code}",
        "error_detail": None,
    }


def _check_label(label: str, env_map: Dict[str, str], timeout_sec: float, public_probe: Dict[str, Any]) -> Dict[str, Any]:
    creds = _resolve_label_creds(label, env_map)
    read_present = bool(creds["read_key"] and creds["read_secret"])
    ws_present = bool(creds["ws_key"] and creds["ws_secret"])
    row: Dict[str, Any] = {
        "label": label,
        "read_creds_present": read_present,
        "read_source": creds["read_source"] or None,
        "ws_creds_present": ws_present,
        "ws_source": creds["ws_source"] or None,
        "connectivity_ok": False,
        "status": "missing_creds",
        "ret_code": None,
        "ret_msg": None,
        "status_code": None,
        "elapsed_ms": None,
        "equity_usdt": None,
        "content_type": None,
        "body_preview": None,
        "diagnosis": None,
        "error_detail": None,
        "environment_public_ok": bool(public_probe.get("ok")),
        "environment_diagnosis": public_probe.get("diagnosis"),
    }
    if not read_present:
        return row

    try:
        probe = _wallet_balance_probe(str(creds["read_key"]), str(creds["read_secret"]), timeout_sec=timeout_sec)
        row.update(probe)
        row["connectivity_ok"] = bool(probe["ok"])
        if probe["ok"]:
            row["status"] = "ok"
        elif not public_probe.get("ok"):
            row["status"] = "env_blocked"
        elif probe.get("ret_code") not in (None, 0):
            row["status"] = "auth_failed"
        else:
            row["status"] = "request_failed"
    except Exception as exc:
        row["status"] = "env_blocked" if not public_probe.get("ok") else "request_failed"
        row["ret_msg"] = str(exc)
        row["error_detail"] = f"{type(exc).__name__}: {exc}"
    return row


def build_report(labels: Iterable[str], env_map: Dict[str, str], timeout_sec: float) -> Dict[str, Any]:
    public_probe = _public_time_probe(timeout_sec)
    rows = [_check_label(label, env_map, timeout_sec, public_probe) for label in labels]
    return {
        "schema_version": "bybit.connectivity.check.v1",
        "generated_ms": int(time.time() * 1000),
        "bybit_base": BYBIT_BASE,
        "public_probe": public_probe,
        "rows": rows,
        "ok_count": sum(1 for row in rows if row.get("connectivity_ok")),
        "missing_count": sum(1 for row in rows if row.get("status") == "missing_creds"),
        "failed_count": sum(1 for row in rows if row.get("status") not in {"ok", "missing_creds"}),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Verify Bybit API credentials and authenticated connectivity per Flashback label.")
    ap.add_argument("--env-file", default=str(ENV_DEFAULT), help="env file to load before checking connectivity")
    ap.add_argument("--labels", nargs="*", default=[], help="labels to check (default: flashback01..flashback10)")
    ap.add_argument("--include-main", action="store_true", help="also verify the main account")
    ap.add_argument("--include-disabled", action="store_true", help="include disabled labels from subaccounts.yaml")
    ap.add_argument("--timeout-sec", type=float, default=10.0, help="HTTP timeout for Bybit probes")
    ap.add_argument("--strict-creds", action="store_true", help="exit non-zero if any label is missing credentials")
    ap.add_argument("--strict-connectivity", action="store_true", help="exit non-zero if any label fails the authenticated probe")
    args = ap.parse_args()

    env_map = _load_env_file(Path(args.env_file))
    for key, value in env_map.items():
        os.environ.setdefault(key, value)

    labels = _selected_labels(args.labels, include_main=bool(args.include_main), include_disabled=bool(args.include_disabled))
    if not labels:
        raise SystemExit("No labels selected for Bybit connectivity check.")

    report = build_report(labels, env_map, timeout_sec=max(1.0, float(args.timeout_sec)))
    print(json.dumps(report, indent=2))

    missing = int(report["missing_count"])
    failed = int(report["failed_count"])
    if args.strict_creds and missing > 0:
        return 2
    if args.strict_connectivity and failed > 0:
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
