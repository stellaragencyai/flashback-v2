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
    while tag in tags:
        tags.remove(tag)

def _add_tag(tags: list, tag: str) -> None:
    if tag not in tags:
        tags.append(tag)

def _promote_from_raw(event: Dict[str, Any]) -> Tuple[bool, bool]:
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
    if order_link_id and not _safe_str(event.get("trade_id")).strip():
        event["trade_id"] = order_link_id
        promoted = True

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

def repair_file(src: Path) -> dict:
    out = src.with_name("outcomes_raw.repaired.jsonl")
    rej = src.with_name("outcomes_raw.repaired.rejects.jsonl")
    bak = src.with_suffix(src.suffix + ".bak_2026-01-09")

    total = wrote = rejects = 0
    promoted_execid = cleared_weak = unit_tagged = unit_cleared = 0

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

            if is_test:
                _add_tag(tags, "synthetic:test_outcome")
                unit_tagged += 1
                if "weak_linkage:no_execution_id" in tags:
                    _remove_tag(tags, "weak_linkage:no_execution_id")
                    unit_cleared += 1

            promoted, had_raw_exec = _promote_from_raw(event)
            if had_raw_exec and _safe_str(event.get("execution_id")).strip():
                if promoted:
                    promoted_execid += 1
                if "weak_linkage:no_execution_id" in tags:
                    _remove_tag(tags, "weak_linkage:no_execution_id")
                    cleared_weak += 1

            f_out.write(json.dumps(event, ensure_ascii=False) + "\n")
            wrote += 1

    # swap in place with backup
    src.replace(bak)          # move original to backup
    out.replace(src)          # move repaired into place

    return {
        "file": str(src),
        "backup": str(bak),
        "total": total,
        "wrote": wrote,
        "rejects": rejects,
        "promoted_execid_rows": promoted_execid,
        "cleared_weak_after_promo": cleared_weak,
        "unit_test_rows_tagged": unit_tagged,
        "unit_test_rows_cleared_weak": unit_cleared,
    }

def main():
    root = Path(r"C:\Flashback\state\ai_events")
    results = []
    for d in root.iterdir():
        if not d.is_dir():
            continue
        f = d / "outcomes_raw.jsonl"
        if f.exists():
            results.append(repair_file(f))

    print("=== BATCH REPAIR PER-ACCOUNT outcomes_raw.jsonl ===")
    for r in results:
        print(f"- {r['file']}")
        print(f"  backup={r['backup']}")
        print(f"  total={r['total']} wrote={r['wrote']} rejects={r['rejects']}")
        print(f"  promoted_execid={r['promoted_execid_rows']} cleared_weak={r['cleared_weak_after_promo']}")
        print(f"  unit_tagged={r['unit_test_rows_tagged']} unit_cleared_weak={r['unit_test_rows_cleared_weak']}")

if __name__ == "__main__":
    main()
