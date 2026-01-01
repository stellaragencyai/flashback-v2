#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Flashback — Project Metrics (Repo-based truth)

Reads:
- docs/phase_checklists/phase_*.yaml

Outputs:
- per-phase completion (DONE / total)
- overall completion (simple average)

Optional:
- --write  → updates docs/flashback_progress.yaml using checklist truth

Usage:
  python -m app.tools.project_metrics
  python -m app.tools.project_metrics --write
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import List, Tuple
import sys

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
    # app/tools/project_metrics.py -> app/tools -> app -> repo root
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
            raise ValueError(
                f"Invalid status '{status}' in {path} (use DONE/NOT_DONE)"
            )

        total += 1
        if status == "DONE":
            done += 1

    return phase_title, total, done


def _phase_number_from_filename(path: Path) -> int:
    # phase_7_execution.yaml -> 7
    try:
        parts = path.stem.split("_")
        return int(parts[1])
    except Exception as e:
        raise ValueError(f"Bad phase checklist filename (expected phase_<n>_*.yaml): {path.name}") from e


def write_progress_yaml(root: Path, rows: List[PhaseRow], overall_pct: float) -> Path:
    """
    Writes/updates docs/flashback_progress.yaml based purely on checklist truth.
    """
    progress_path = root / "docs" / "flashback_progress.yaml"

    progress = {}
    if progress_path.exists():
        progress = load_yaml(progress_path)

    # Top-level metadata
    progress["project"] = progress.get("project", "Flashback Trading AI")
    progress["last_updated"] = str(date.today())

    overall = progress.get("overall", {})
    if not isinstance(overall, dict):
        overall = {}

    overall["weighted_completion_pct"] = overall_pct
    overall.setdefault(
        "note",
        "Percentages are checklist-derived. DONE only counts when evidence is real and committed.",
    )
    progress["overall"] = overall

    # Phases mapping (keep existing keys if present, otherwise create deterministic ones)
    phases = progress.get("phases", {})
    if not isinstance(phases, dict):
        phases = {}

    existing_keys = list(phases.keys())

    for idx, r in enumerate(rows, start=1):
        # Preserve existing key ordering if it exists; otherwise use deterministic key.
        phase_key = existing_keys[idx - 1] if len(existing_keys) >= idx else f"phase_{idx}"

        phases.setdefault(phase_key, {})
        phases[phase_key]["name"] = r.phase_title
        phases[phase_key]["completion_pct"] = r.pct
        phases[phase_key]["checklist_total"] = r.total
        phases[phase_key]["checklist_done"] = r.done

    progress["phases"] = phases

    progress_path.write_text(yaml.safe_dump(progress, sort_keys=False), encoding="utf-8")
    return progress_path


def main() -> int:
    root = repo_root()
    checklist_dir = root / "docs" / "phase_checklists"

    files = sorted(
        checklist_dir.glob("phase_*.yaml"),
        key=_phase_number_from_filename
    )
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

    if "--write" in sys.argv:
        path = write_progress_yaml(root, rows, overall)
        print(f"📝 Updated progress file: {path}\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
