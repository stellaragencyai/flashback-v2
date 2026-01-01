from __future__ import annotations

from pathlib import Path

TARGET = Path(r"app\bots\signal_engine.py")

NEW_ENSURE_DEBUG = [
    "# FORCE_DEBUG_CALLSITE_V1: ensure observed.jsonl debug always has real numbers when candles are available",
    "def _ensure_debug(candles, dbg):",
    "    \"\"\"",
    "    Ensure we always have last_close/prev_close/ma,",
    "    but DO NOT drop existing keys like 'regime', 'setup', 'signal_origin'.",
    "    \"\"\"",
    "    base = dbg if isinstance(dbg, dict) else {}",
    "    try:",
    "        lc = base.get('last_close')",
    "        pc = base.get('prev_close')",
    "        ma = base.get('ma')",
    "        if (lc is not None) and (pc is not None) and (ma is not None):",
    "            return base",
    "",
    "        _side, computed = compute_simple_signal(candles)",
    "        if isinstance(computed, dict) and computed:",
    "            merged = dict(base)",
    "            for k in ('last_close','prev_close','ma'):",
    "                if merged.get(k) is None and computed.get(k) is not None:",
    "                    merged[k] = computed.get(k)",
    "            if not merged.get('reason') and computed.get('reason'):",
    "                merged['reason'] = computed.get('reason')",
    "            return merged",
    "    except Exception:",
    "        pass",
    "    return base",
    "",
]

def _patch_ensure_debug(lines: list[str]) -> tuple[list[str], bool]:
    # Find the FORCE_DEBUG_CALLSITE_V1 marker then replace the whole def _ensure_debug block
    start = None
    for i, line in enumerate(lines):
        if line.strip().startswith("# FORCE_DEBUG_CALLSITE_V1"):
            start = i
            break
    if start is None:
        return lines, False

    # Find end of function: first non-indented top-level line after def block that is not blank/comment
    # We'll start scanning from start+1 and find the next line that begins at column 0 AND starts with "def " or another top-level block,
    # but only after we've passed the "def _ensure_debug" line.
    end = None
    saw_def = False
    for j in range(start, len(lines)):
        if lines[j].startswith("def _ensure_debug("):
            saw_def = True
            continue
        if saw_def:
            # a new top-level def indicates end
            if lines[j].startswith("def ") and not lines[j].startswith("def _ensure_debug("):
                end = j
                break
            # also break on other top-level markers (rare)
            if (not lines[j].startswith(" ")) and (lines[j].strip().startswith("class ") or lines[j].strip().startswith("if __name__")):
                end = j
                break

    if end is None:
        # Replace to EOF if we couldn't find a clean end (still safe)
        end = len(lines)

    new_lines = lines[:start] + NEW_ENSURE_DEBUG + lines[end:]
    return new_lines, True

def _patch_append_signal_jsonl(lines: list[str]) -> tuple[list[str], bool]:
    # Inject '"regime": debug.get("regime"),' after '"raw_reason": reason,' inside the payload debug dict.
    changed = False
    for i in range(len(lines)):
        if '"raw_reason": reason,' in lines[i]:
            # Check next few lines to avoid double insert
            window = "\n".join(lines[i:i+6])
            if '"regime": debug.get("regime")' in window:
                return lines, False
            indent = lines[i].split('"raw_reason"')[0]
            lines.insert(i+1, f'{indent}"regime": debug.get("regime"),')
            changed = True
            break
    return lines, changed

def main() -> int:
    if not TARGET.exists():
        print(f"FAIL: missing {TARGET}")
        return 2

    src = TARGET.read_text(encoding="utf-8", errors="ignore").splitlines()

    src2, ok1 = _patch_ensure_debug(src)
    src3, ok2 = _patch_append_signal_jsonl(src2)

    TARGET.write_text("\n".join(src3) + "\n", encoding="utf-8", newline="\n")

    print(f"OK: patched signal_engine.py ensure_debug_replaced={ok1} append_debug_regime_added={ok2}")

    # Compile check
    try:
        compile(TARGET.read_text(encoding="utf-8", errors="ignore"), str(TARGET), "exec")
        print("PASS: signal_engine.py compiles after patch")
        return 0
    except Exception as e:
        print("FAIL: signal_engine.py does not compile after patch:", e)
        return 1

if __name__ == "__main__":
    raise SystemExit(main())
