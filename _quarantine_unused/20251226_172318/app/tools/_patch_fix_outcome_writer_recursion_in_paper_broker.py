from __future__ import annotations

from pathlib import Path
import re

TARGET = Path(r"app\sim\paper_broker.py")

def die(msg: str) -> None:
    raise SystemExit(msg)

def main() -> None:
    if not TARGET.exists():
        die(f"FAIL: missing {TARGET}")

    s = TARGET.read_text(encoding="utf-8", errors="ignore")

    # Ensure we have the helper
    m = re.search(r"^def\s+_write_outcome_safe\s*\(\*\*kwargs\)\s*:[\s\S]*?^\s*return\s+False\s*$",
                  s, flags=re.MULTILINE)
    if not m:
        die("FAIL: could not find def _write_outcome_safe(**kwargs) block")

    block = m.group(0)

    # 1) Ensure we have a stable alias to the imported writer function
    # Add `_OUTCOME_WRITER_FN = write_outcome_from_paper_close` after the import block if missing
    if "_OUTCOME_WRITER_FN" not in s:
        # Insert after the line that sets write_outcome_from_paper_close = None (or after import try/except)
        ins = re.search(
            r"^try:\s*\n\s*from app\.ai\.outcome_writer import write_outcome_from_paper_close.*?\nexcept Exception:\s*\n\s*write_outcome_from_paper_close\s*=\s*None\s*#\s*type:\s*ignore\s*\n",
            s,
            flags=re.MULTILINE | re.DOTALL
        )
        if not ins:
            # fallback: any import block without the comment
            ins = re.search(
                r"^# --- Outcome v1 writer \(fail-soft\) ---\s*\ntry:\s*\n\s*from app\.ai\.outcome_writer import write_outcome_from_paper_close.*?\nexcept Exception:\s*\n\s*write_outcome_from_paper_close\s*=\s*None.*?\n",
                s,
                flags=re.MULTILINE | re.DOTALL
            )
        if not ins:
            die("FAIL: could not locate outcome_writer import block to add _OUTCOME_WRITER_FN alias")

        insert_at = ins.end()
        alias_line = "\n# Stable alias to avoid recursion from wrapper replacements\n_OUTCOME_WRITER_FN = write_outcome_from_paper_close  # type: ignore\n\n"
        s = s[:insert_at] + alias_line + s[insert_at:]

    # 2) Fix the helper to call _OUTCOME_WRITER_FN (real writer), not _write_outcome_safe / itself
    # Replace any line like: _write_outcome_safe(**filtered) OR write_outcome_from_paper_close(**filtered)
    s = re.sub(
        r"(\n\s*)(?:_write_outcome_safe|write_outcome_from_paper_close)\s*\(\s*\*\*filtered\s*\)",
        r"\1_OUTCOME_WRITER_FN(**filtered)  # type: ignore",
        s
    )

    # 3) Also ensure the helper checks the alias, not the wrapper name
    s = re.sub(
        r"if\s+write_outcome_from_paper_close\s+is\s+None\s*:",
        "if _OUTCOME_WRITER_FN is None:",
        s
    )

    # 4) And ensure signature() is taken from the alias
    s = re.sub(
        r"inspect\.signature\(\s*write_outcome_from_paper_close\s*\)",
        "inspect.signature(_OUTCOME_WRITER_FN)",
        s
    )

    TARGET.write_text(s, encoding="utf-8")
    print("OK: patched paper_broker.py (fixed outcome writer recursion; wrapper calls real function)")

if __name__ == "__main__":
    main()
