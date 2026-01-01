#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Learning Contract v1 (Phase 6)

Defines:
- Canonical derived artifact paths for Phase 6
- Schema version constants
- Safety rules: Phase 6 is read-only over Phase 5 memory

This module MUST be import-stable.
No heavy imports. No Bybit calls. Fail-soft.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


LEARNING_SCHEMA_VERSION = 1
ADVISORY_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class LearningPaths:
    root: Path
    learning_dir: Path

    # Core learning artifacts
    learning_sqlite_path: Path
    memory_stats_jsonl_path: Path
    drift_report_json_path: Path

    # Advisory artifacts (derived from learning.sqlite)
    advisory_jsonl_path: Path
    advisory_rankings_json_path: Path

    @classmethod
    def default(cls) -> "LearningPaths":
        root = Path(__file__).resolve().parents[2]
        state = root / "state"
        learning_dir = state / "ai_learning"
        return cls(
            root=root,
            learning_dir=learning_dir,
            learning_sqlite_path=(learning_dir / "learning.sqlite"),
            memory_stats_jsonl_path=(learning_dir / "memory_stats_v1.jsonl"),
            drift_report_json_path=(learning_dir / "drift_report_v1.json"),
            advisory_jsonl_path=(learning_dir / "advisory_v1.jsonl"),
            advisory_rankings_json_path=(learning_dir / "advisory_rankings_v1.json"),
        )
