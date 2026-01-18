from pathlib import Path
import re

p = Path(r".\app\ai\ai_events_spine.py")
s = p.read_text(encoding="utf-8")

# -----------------------------
# Helper to insert once
# -----------------------------
def insert_before(def_name: str, insert_text: str) -> str:
    pat = re.compile(rf"^def\s+{re.escape(def_name)}\s*\(.*?\)\s*->\s*None\s*:\s*$", re.M)
    m = pat.search(s)
    if not m:
        raise SystemExit(f"ERROR: def not found: {def_name}")
    return s[:m.start()] + insert_text + "\n" + s[m.start():]

# -----------------------------
# 1) Insert helper before _enforce_chain_integrity (if missing)
# -----------------------------
if "_promote_outcome_linkage_from_raw" not in s:
    helper = r'''
def _promote_outcome_linkage_from_raw(event: Dict[str, Any]) -> None:
    """Promote linkage fields from payload.extra.raw onto the top-level event (and payload.extra).

    Execution-derived outcome_records typically carry exec linkage under payload.extra.raw,
    but chain-integrity checks expect execution_id at top-level.
    """
    try:
        if not isinstance(event, dict):
            return
        et = event.get("event_type")
        if et not in ("outcome_record", "outcome_enriched"):
            return

        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        extra = payload.get("extra") if isinstance(payload.get("extra"), dict) else {}
        raw = extra.get("raw") if isinstance(extra.get("raw"), dict) else None
        if not raw:
            return

        exec_id = raw.get("execId") or raw.get("exec_id") or raw.get("execution_id")
        order_id = raw.get("orderId") or raw.get("order_id")
        order_link_id = raw.get("orderLinkId") or raw.get("order_link_id") or raw.get("orderLinkID")

        # Promote to top-level
        if exec_id and not event.get("execution_id"):
            event["execution_id"] = exec_id
        if order_id and not event.get("order_id"):
            event["order_id"] = order_id
        if order_link_id and not event.get("order_link_id"):
            event["order_link_id"] = order_link_id

        # If trade_id is missing, fall back to orderLinkId
        if order_link_id and not str(event.get("trade_id") or "").strip():
            event["trade_id"] = order_link_id

        # Also promote into payload.extra for downstream consumers
        if exec_id and not extra.get("execution_id"):
            extra["execution_id"] = exec_id
        if order_id and not extra.get("order_id"):
            extra["order_id"] = order_id
        if order_link_id and not extra.get("order_link_id"):
            extra["order_link_id"] = order_link_id

        payload["extra"] = extra
        event["payload"] = payload
    except Exception:
        return
'''.lstrip("\n")
    # Insert right before _enforce_chain_integrity
    pat_enforce = re.compile(r"^def\s+_enforce_chain_integrity\s*\(.*?\)\s*->\s*None\s*:\s*$", re.M)
    m = pat_enforce.search(s)
    if not m:
        raise SystemExit("ERROR: def _enforce_chain_integrity not found")
    s = s[:m.start()] + helper + "\n" + s[m.start():]

# -----------------------------
# 2) Wire promote call into _process_canonical_event before _enforce_chain_integrity
# -----------------------------
def wire_promote_call(s: str) -> str:
    # Find the _process_canonical_event function block
    m = re.search(r"^def\s+_process_canonical_event\s*\(.*?\)\s*->\s*None\s*:\s*$", s, re.M)
    if not m:
        raise SystemExit("ERROR: def _process_canonical_event not found")

    # Within that function, locate the first occurrence of _enforce_chain_integrity(event)
    # and ensure _promote_outcome_linkage_from_raw(event) is immediately before it.
    # This is safe even if there are blank lines/comments between attach/stamp/enforce.

    # Limit to the function body by slicing from def to next top-level def/class
    start = m.start()
    body_start = m.end()
    tail = s[body_start:]
    n = re.search(r"^(def|class)\s+\w+\s*\(", tail, re.M)
    end = body_start + (n.start() if n else len(tail))
    block = s[start:end]

    if "_enforce_chain_integrity(event)" not in block:
        raise SystemExit("ERROR: _enforce_chain_integrity(event) not found inside _process_canonical_event")

    # If already wired, do nothing
    if "_promote_outcome_linkage_from_raw(event)" in block:
        return s

    # Insert promote call directly before the first _enforce_chain_integrity(event)
    block2 = re.sub(
        r"(?m)^(?P<indent>\s*)_enforce_chain_integrity\(event\)\s*$",
        r"\g<indent>_promote_outcome_linkage_from_raw(event)\n\g<indent>_enforce_chain_integrity(event)",
        block,
        count=1
    )

    return s[:start] + block2 + s[end:]

s = wire_promote_call(s)

# -----------------------------
# 3) Patch weak_linkage tagging logic inside _enforce_chain_integrity
# -----------------------------
# We replace the specific outcome tagging block by searching for the first
# 'if et in ("outcome_record", "outcome_enriched")' block and rewriting it.
m_enforce = re.search(r"^def\s+_enforce_chain_integrity\s*\(.*?\)\s*->\s*None\s*:\s*$", s, re.M)
if not m_enforce:
    raise SystemExit("ERROR: def _enforce_chain_integrity not found (post-insert)")

enforce_start = m_enforce.start()
enforce_body_start = m_enforce.end()
tail = s[enforce_body_start:]
n = re.search(r"^(def|class)\s+\w+\s*\(", tail, re.M)
enforce_end = enforce_body_start + (n.start() if n else len(tail))
enforce_block = s[enforce_start:enforce_end]

# Find the existing outcome tagging stanza (your file likely has this exact pattern)
pat = re.compile(
    r"(?ms)^\s*if et in \(\s*\"outcome_record\"\s*,\s*\"outcome_enriched\"\s*\)\s*:\s*"
    r"\n\s*if not _safe_str\(event\.get\(\"execution_id\"\)\)\s*:\s*"
    r"\n\s*tags = event\.get\(\"tags\"\)\s*"
    r"\n\s*if not isinstance\(tags, list\)\s*:\s*"
    r"\n\s*tags = \[\]\s*"
    r"\n\s*tags\.append\(\"weak_linkage:no_execution_id\"\)\s*"
    r"\n\s*event\[\s*\"tags\"\s*\]\s*=\s*tags\s*"
)
m = pat.search(enforce_block)
if not m:
    # If it doesn't match, fall back to a simpler search and do a safer insert
    if 'weak_linkage:no_execution_id' not in enforce_block:
        raise SystemExit("ERROR: could not locate outcome weak_linkage block to patch")
    raise SystemExit("ERROR: outcome weak_linkage block exists but pattern mismatch; need manual patch")

replacement = r'''
    if et in ("outcome_record", "outcome_enriched"):
        # Promote raw linkage before deciding if this is truly weak linkage
        _promote_outcome_linkage_from_raw(event)

        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        extra = payload.get("extra") if isinstance(payload.get("extra"), dict) else {}
        schema = str(extra.get("schema_version") or "").strip().lower()

        # Unit tests / synthetic outcomes shouldn't spam weak-linkage tags
        if schema in ("unit_test", "unittest", "test"):
            tags = event.get("tags")
            if not isinstance(tags, list):
                tags = []
            if "synthetic:test_outcome" not in tags:
                tags.append("synthetic:test_outcome")
            event["tags"] = tags
            return

        # Real outcomes must have execution_id to be considered strongly linkable
        if not _safe_str(event.get("execution_id")):
            tags = event.get("tags")
            if not isinstance(tags, list):
                tags = []
            if "weak_linkage:no_execution_id" not in tags:
                tags.append("weak_linkage:no_execution_id")
            event["tags"] = tags
'''.rstrip("\n")

enforce_block2 = enforce_block[:m.start()] + replacement + enforce_block[m.end():]
s = s[:enforce_start] + enforce_block2 + s[enforce_end:]

p.write_text(s, encoding="utf-8")
print("OK: patched ai_events_spine.py (regex-based)")
