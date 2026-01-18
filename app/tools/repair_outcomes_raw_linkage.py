#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Tuple


def _safe_str(x: Any) -> str:
    try:
        return str(x) if x is not None else ""
    except Exception:
        return ""


def _is_test_schema(schema: str) -> bool:
    s = (schema or "").strip().lower()
    return s in ("unit_test", "unittest", "test") or ("unit_test" in s) or (s == "unit") or (s.endswith("_test"))


def _ensure_list_tags(event: Dict[str, Any]) -> list:
    tags = event.get("tags")
    if not isinstance(tags, list):
        tags = []
    event["tags"] = tags
    return tags


def _remove_tag(tags: list, tag: str) -> None:
    # remove all occurrences (some rows may have duplicates)
    while tag in tags:
        tags.remove(tag)


def _add_tag(tags: list, tag: str) -> None:
    if tag not in tags:
        tags.append(tag)


def _promote_from_raw(event: Dict[str, Any]) -> Tuple[bool, bool]:
    """
    Returns (promoted_any, had_raw_exec)
    """
    payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
    extra = payload.get("extra") if isinstance(payload.get("extra"), dict) else {}
    raw = extra.get("raw") if isinstance(extra.get("raw"), dict) else None
    if not raw:
        return (False, False)

    exec_id = raw.get("execId") or raw.get("exec_id") or raw.get("execution_id")
    order_id = raw.get("orderId") or raw.get("order_id")
    order_link_id = raw.get("orderLinkId") or raw.get("order_link_id") or raw.get("orderLinkID")

    had_raw_exec = bool(exec_id)
    promoted = False

    if exec_id and not event.get("execution_id"):
        event["execution_id"] = exec_id
        promoted = True

    if order_id and not event.get("order_id"):
        event["order_id"] = order_id
        promoted = True

    if order_link_id and not event.get("order_link_id"):
        event["order_link_id"] = order_link_id
        promoted = True

    # If trade_id is missing/empty, fall back to orderLinkId
    if order_link_id and not _safe_str(event.get("trade_id")).strip():
        event["trade_id"] = order_link_id
        promoted = True

    # Also promote into payload.extra (keeps downstream consistent)
    if exec_id and not extra.get("execution_id"):
        extra["execution_id"] = exec_id
        promoted = True
    if order_id and not extra.get("order_id"):
        extra["order_id"] = order_id
        promoted = True
    if order_link_id and not extra.get("order_link_id"):
        extra["order_link_id"] = order_link_id
        promoted = True

    payload["extra"] = extra
    event["payload"] = payload

    return (promoted, had_raw_exec)


def main() -> int:
    src = Path(r".\state\ai_events\outcomes_raw.jsonl")
    if not src.exists():
        print(f"ERROR: not found: {src}")
        return 2

    out = src.with_name("outcomes_raw.repaired.jsonl")
    rej = src.with_name("outcomes_raw.repaired.rejects.jsonl")

    total = 0
    wrote = 0
    rejects = 0

    promoted_execid = 0
    cleared_weak_due_to_promotion = 0
    unit_test_tagged = 0
    unit_test_cleared_weak = 0

    with src.open("r", encoding="utf-8") as f_in, \
         out.open("w", encoding="utf-8", newline="\n") as f_out, \
         rej.open("w", encoding="utf-8", newline="\n") as f_rej:

        for line in f_in:
            total += 1
            line = line.strip()
            if not line:
                continue

            try:
                event = json.loads(line)
                if not isinstance(event, dict):
                    raise ValueError("not a json object")
            except Exception as e:
                rejects += 1
                f_rej.write(json.dumps({"reason": "bad_json", "error": str(e), "line": line}, ensure_ascii=False) + "\n")
                continue

            tags = _ensure_list_tags(event)

            payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
            extra = payload.get("extra") if isinstance(payload.get("extra"), dict) else {}
            schema = _safe_str(extra.get("schema_version")).strip()

            is_test = _is_test_schema(schema)

            # 1) If test schema: ensure synthetic tag, remove weak linkage tag
            if is_test:
                _add_tag(tags, "synthetic:test_outcome")
                unit_test_tagged += 1
                if "weak_linkage:no_execution_id" in tags:
                    _remove_tag(tags, "weak_linkage:no_execution_id")
                    unit_test_cleared_weak += 1

            # 2) Promote from raw linkage if present
            promoted, had_raw_exec = _promote_from_raw(event)

            if had_raw_exec and _safe_str(event.get("execution_id")).strip():
                # We successfully have exec linkage now.
                if promoted and event.get("execution_id"):
                    promoted_execid += 1

                if "weak_linkage:no_execution_id" in tags:
                    _remove_tag(tags, "weak_linkage:no_execution_id")
                    cleared_weak_due_to_promotion += 1

            f_out.write(json.dumps(event, ensure_ascii=False) + "\n")
            wrote += 1

    print("=== REPAIR outcomes_raw.jsonl ===")
    print("SRC =", str(src))
    print("OUT =", str(out))
    print("REJ =", str(rej))
    print("TOTAL_LINES =", total)
    print("WROTE =", wrote)
    print("REJECTS =", rejects)
    print("--- FIX COUNTS ---")
    print("PROMOTED_EXECID_ROWS =", promoted_execid)
    print("CLEARED_WEAK_TAG_AFTER_PROMOTION =", cleared_weak_due_to_promotion)
    print("UNIT_TEST_ROWS_TAGGED_SYNTHETIC =", unit_test_tagged)
    print("UNIT_TEST_ROWS_CLEARED_WEAK =", unit_test_cleared_weak)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
