#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from app.core.ai_decision_types import DECISION_SCHEMA_VERSION


REQUIRED_KEYS = ("schema_version", "ts", "decision", "tier_used", "gates")


def validate_pilot_decision(d: Dict[str, Any]) -> Tuple[bool, List[str]]:
    errs: List[str] = []
    if not isinstance(d, dict):
        return (False, ["not_a_dict"])

    sv = d.get("schema_version")
    if sv != DECISION_SCHEMA_VERSION:
        errs.append(f"bad_schema_version({sv} != {DECISION_SCHEMA_VERSION})")

    for k in REQUIRED_KEYS:
        if k not in d:
            errs.append(f"missing:{k}")

    if "ts" in d:
        try:
            int(d["ts"])
        except Exception:
            errs.append("bad_ts")

    if "gates" in d and not isinstance(d.get("gates"), dict):
        errs.append("gates_not_dict")

    return (len(errs) == 0, errs)
