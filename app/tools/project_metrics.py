#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Flashback — Project Metrics (Repo-based truth)

Reads:
- docs/phase_checklists/*.yaml

Outputs:
- per-phase completion (DONE / total)
- overall completion (simple average)

Usage:
  python -m app.tools.project_metrics
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

import yaml  # type: ignore


VALID_STATUSES = {"DONE", "NOT_DONE"}


@dataclass
class PhaseRow:
    phase_file: str
    phase_title: str
    total: int
    done: int

    @property
    def pct(self) -> float:
        return round((self.done / self.total) * 100.0, 2) if self.total else 0.0


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def load_yaml(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ValueError(f"YAML must be a mapping in {path}")
    return data


def load_checklist(path: Path) -> Tuple[str, int, int]:
    data = load_yaml(path)
    phase_title = str(data.get("phase", path.stem))
    checklist = data.get("checklist", [])
    if not isinstance(checklist, list):
        raise ValueError(f"'checklist' must be a list in {path}")

    total = 0
    done = 0
    seen_ids = set()

    for item in checklist:
        if not isinstance(item, dict):
            raise ValueError(f"Checklist item must be dict in {path}: {item}")
        _id = str(item.get("id", "")).strip()
        status = str(item.get("status", "")).strip().upper()

        if not _id:
            raise ValueError(f"Missing id in {path}: {item}")
        if _id in seen_ids:
            raise ValueError(f"Duplicate id '{_id}' in {path}")
        seen_ids.add(_id)

        if status not in VALID_STATUSES:
            raise ValueError(f"Invalid status '{status}' in {path} (use DONE/NOT_DONE)")

        total += 1
        if status == "DONE":
            done += 1

    return phase_title, total, done


def main() -> int:
    root = repo_root()
    checklist_dir = root / "docs" / "phase_checklists"

    files = sorted(checklist_dir.glob("phase_*.yaml"))
    if not files:
        raise FileNotFoundError(f"No phase checklist files found in {checklist_dir}")

    rows: List[PhaseRow] = []
    for f in files:
        title, total, done = load_checklist(f)
        rows.append(PhaseRow(phase_file=f.name, phase_title=title, total=total, done=done))

    overall = round(sum(r.pct for r in rows) / len(rows), 2)

    print("\n📊 FLASHBACK METRICS (Checklist truth)\n")
    print(f"Repo root: {root}")
    print(f"Checklist dir: {checklist_dir}\n")

    for idx, r in enumerate(rows, start=1):
        print(f"{idx:02d}) {r.phase_title}")
        print(f"    File: {r.phase_file}")
        print(f"    Done: {r.done}/{r.total}  →  {r.pct}%\n")

    print(f"🎯 Overall completion (simple average): {overall}%\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
