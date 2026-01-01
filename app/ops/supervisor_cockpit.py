from __future__ import annotations

import json
import os
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict


ROOT = Path(__file__).resolve().parents[2]
STATE = ROOT / "state"
OPS = STATE / "ops_snapshot.json"
ORCH = STATE / "orchestrator_state.json"
DASH = STATE / "dashboard_snapshot.json"
MANIFEST = ROOT / "config" / "fleet_manifest.yaml"


def clear() -> None:
    os.system("cls" if os.name == "nt" else "clear")


def load_json(p: Path) -> dict:
    try:
        return json.loads(p.read_text(encoding="utf-8", errors="ignore") or "{}")
    except Exception:
        return {}


def load_manifest_rows() -> list[dict[str, Any]]:
    try:
        if not MANIFEST.exists():
            return []
        import yaml  # type: ignore

        d = yaml.safe_load(MANIFEST.read_text(encoding="utf-8", errors="ignore")) or {}
        fleet = d.get("fleet") or []
        if not isinstance(fleet, list):
            return []
        out: list[dict[str, Any]] = []
        for r in fleet:
            if isinstance(r, dict):
                out.append(r)
        return out
    except Exception:
        return []


def mode_ok(mode: str) -> bool:
    m = (mode or "").strip().upper()
    return m not in ("", "OFF", "DISABLED", "NONE", "NULL")


def trunc(s: str, n: int) -> str:
    s = str(s or "")
    return s if len(s) <= n else s[: n - 1] + "…"


def count(v: Any) -> int:
    if isinstance(v, list):
        return len(v)
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, int):
        return v
    return 0


def normalize_label(comp_key: str) -> str:
    # common formats: "something:flashback01" or "flashback01"
    k = str(comp_key or "")
    if ":" in k:
        return k.split(":")[-1].strip()
    return k.strip()


def normalize_account_label(v: Any) -> str:
    s = str(v or "").strip().lower()
    if not s:
        return ""
    if s == "main":
        return "main"
    if s.startswith("flashback_"):
        tail = s.replace("flashback_", "")
        if tail.isdigit():
            return f"flashback{int(tail):02d}"
        return s.replace("_", "")
    if s.startswith("flashback"):
        tail = s.replace("flashback", "").replace("_", "")
        if tail.isdigit():
            return f"flashback{int(tail):02d}"
        return s.replace("_", "")
    return s.replace("_", "")


while True:
    clear()
    ops = load_json(OPS)
    orch = load_json(ORCH)
    dash = load_json(DASH)
    rows = load_manifest_rows()

    components: Dict[str, Any] = ops.get("components", {}) if isinstance(ops, dict) else {}
    procs: Dict[str, Any] = orch.get("procs", {}) if isinstance(orch, dict) else {}

    dash_accounts: Dict[str, Any] = {}
    if isinstance(dash, dict) and isinstance(dash.get("accounts"), dict):
        for k, v in dash["accounts"].items():
            nk = normalize_account_label(k)
            if nk:
                dash_accounts[nk] = v if isinstance(v, dict) else {}

    accounts = defaultdict(
        lambda: {
            "enabled": 0,
            "running": 0,
            "dead": 0,
            "mode": "?",
            "trades": 0,
            "pnl": 0.0,
            "winr": 0.0,
            "fresh_s": "?",
            "last_ms": 0,
            "sup": "OFF",
        }
    )

    now_ms = int(time.time() * 1000)

    # 1) Seed accounts from manifest so MODE is never "?" just because ops_snapshot is empty/stale.
    manifest_labels: set[str] = set()
    for r in rows:
        label = normalize_account_label(r.get("account_label") or "")
        if not label:
            continue
        manifest_labels.add(label)
        a = accounts[label]

        m = str(r.get("automation_mode") or "").strip()
        if mode_ok(m):
            a["mode"] = m.upper()
        else:
            a["mode"] = "OFF" if (m.strip().upper() == "OFF") else a["mode"]

    # 2) Merge in truth snapshot (trades/pnl/winrate/freshness)
    for label, d in (dash_accounts.items() if isinstance(dash_accounts, dict) else []):
        a = accounts[label]
        try:
            n = int(d.get("n", 0) or 0)
        except Exception:
            n = 0
        try:
            pnl = float(d.get("pnl_sum", 0.0) or 0.0)
        except Exception:
            pnl = 0.0
        try:
            winr = float(d.get("winrate", 0.0) or 0.0) * 100.0
        except Exception:
            winr = 0.0
        try:
            last_ts = int(d.get("last_ts_ms", 0) or 0)
        except Exception:
            last_ts = 0

        a["trades"] = max(a["trades"], n)
        a["pnl"] = pnl
        a["winr"] = winr
        if last_ts > 0:
            a["last_ms"] = max(a["last_ms"], last_ts)
            a["fresh_s"] = int(max(0, (now_ms - last_ts) / 1000))

    # 3) Merge in ops_snapshot component details (workers + timestamps)
    for comp, data in (components.items() if isinstance(components, dict) else []):
        label = normalize_account_label(normalize_label(comp))
        if not label:
            continue
        d = (data.get("details", {}) if isinstance(data, dict) else {}) or {}
        a = accounts[label]

        a["enabled"] += count(d.get("enabled_workers"))
        a["running"] += count(d.get("running_workers"))
        a["dead"] += count(d.get("dead_workers"))

        a["mode"] = str(d.get("mode", a["mode"]) or a["mode"]).strip() or a["mode"]

        ts_ms = 0
        try:
            ts_ms = int((data.get("ts_ms") if isinstance(data, dict) else 0) or 0)
        except Exception:
            ts_ms = 0
        a["last_ms"] = max(a["last_ms"], ts_ms)
        if a["fresh_s"] == "?" and a["last_ms"]:
            a["fresh_s"] = int(max(0, (now_ms - int(a["last_ms"])) / 1000))

    # 4) Merge in orchestrator liveness and last-checked timestamps (SUP reflects alive=True)
    for label_raw, pinfo in (procs.items() if isinstance(procs, dict) else []):
        label = normalize_account_label(label_raw)
        if not label:
            continue
        a = accounts[label]
        if isinstance(pinfo, dict) and bool(pinfo.get("alive")):
            a["sup"] = "ON"

        for k in ("last_checked_ts_ms", "started_ts_ms"):
            try:
                a["last_ms"] = max(a["last_ms"], int((pinfo.get(k) if isinstance(pinfo, dict) else 0) or 0))
            except Exception:
                pass
        if a["fresh_s"] == "?" and a["last_ms"]:
            a["fresh_s"] = int(max(0, (now_ms - int(a["last_ms"])) / 1000))

    # 5) Ensure we display all manifest labels + any labels present in ops/orch/dash
    display_labels = sorted(set(accounts.keys()) | manifest_labels)

    print("FLASHBACK — SUPERVISOR COCKPIT v0.5 (truth-first)")
    print("=" * 120)
    print(f"{'ACCOUNT':14} {'MODE':8} {'SUP':5} {'WORKERS':15} {'TRADES':8} {'PNL':10} {'WIN%':7} {'FRESH(s)':8}")
    print("-" * 120)

    for label in display_labels:
        a = accounts[label]
        workers = f"{a['running']}/{a['enabled']}/{a['dead']}"
        mode = trunc(str(a["mode"] or "?").upper(), 8)

        pnl_s = f"{float(a['pnl']):.4f}"
        win_s = f"{float(a['winr']):.1f}"

        print(
            f"{trunc(label,14):14} "
            f"{mode:<8} "
            f"{a['sup']:<5} "
            f"{workers:<15} "
            f"{int(a['trades']):<8} "
            f"{pnl_s:<10} "
            f"{win_s:<7} "
            f"{str(a['fresh_s']):<8}"
        )

    time.sleep(2)

# COCKPIT_V0_5
