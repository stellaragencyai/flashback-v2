#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Flashback — Checklist Updater

Updates a phase checklist YAML item by ID:
- sets status (DONE / NOT_DONE)
- appends evidence (string)
- updates evidence_updated (YYYY-MM-DD)

Usage examples:
  python -m app.tools.checklist_update --phase 1 --id P1_CLEANUP_FOSSILS --status DONE --evidence "Moved legacy bots to app/lab/bots_legacy on 2025-12-12"
"""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
from typing import Any, Dict, List

import yaml  # type: ignore


VALID_STATUSES = {"DONE", "NOT_DONE"}


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def phase_file_for_number(n: int) -> Path:
    root = repo_root()
    d = root / "docs" / "phase_checklists"
    matches = sorted(d.glob(f"phase_{n}_*.yaml"))
    if not matches:
        raise FileNotFoundError(f"No checklist file found for phase {n} in {d}")
    if len(matches) > 1:
        raise RuntimeError(f"Multiple checklist files found for phase {n}: {[m.name for m in matches]}")
    return matches[0]


def load_yaml(path: Path) -> Dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ValueError(f"Checklist YAML must be a mapping: {path}")
    return data


def save_yaml(path: Path, data: Dict[str, Any]) -> None:
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", type=int, required=True, help="Phase number (1..10)")
    ap.add_argument("--id", required=True, help="Checklist item id (must match YAML)")
    ap.add_argument("--status", required=True, choices=sorted(VALID_STATUSES), help="DONE or NOT_DONE")
    ap.add_argument("--evidence", default="", help="Evidence string to append")
    args = ap.parse_args()

    path = phase_file_for_number(args.phase)
    data = load_yaml(path)

    checklist = data.get("checklist", [])
    if not isinstance(checklist, list):
        raise ValueError(f"'checklist' must be a list in {path}")

    target = None
    for item in checklist:
        if isinstance(item, dict) and str(item.get("id", "")).strip() == args.id:
            target = item
            break

    if target is None:
        raise KeyError(f"Checklist id '{args.id}' not found in {path}")

    # Update status
    target["status"] = args.status

    # Append evidence
    if args.evidence.strip():
        ev = target.get("evidence", [])
        if isinstance(ev, str):
            ev = [ev]
        if ev is None:
            ev = []
        if not isinstance(ev, list):
            raise ValueError(f"'evidence' must be list or string in {path} for id={args.id}")
        ev.append(args.evidence.strip())
        target["evidence"] = ev
        target["evidence_updated"] = str(date.today())

    save_yaml(path, data)

    print(f"✅ Updated {path.name}: id={args.id} status={args.status}")
    if args.evidence.strip():
        print(f"   + evidence appended: {args.evidence.strip()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
