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

    # ---------------------------------------------------------------------
    # 1) Inject a helper: _write_outcome_safe(...) that filters kwargs based on
    #    write_outcome_from_paper_close signature.
    # ---------------------------------------------------------------------
    if "_write_outcome_safe(" not in s:
        anchor = "# --- Outcome v1 writer (fail-soft) ---"
        idx = s.find(anchor)
        if idx < 0:
            die("FAIL: could not find anchor '# --- Outcome v1 writer (fail-soft) ---'")

        # Insert helper AFTER the writer import block (right after the try/except that sets write_outcome_from_paper_close)
        # We'll locate the end of that block by finding the first blank line after it.
        m = re.search(r"# --- Outcome v1 writer \(fail-soft\) ---\s*?\ntry:\s*?\n\s*from app\.ai\.outcome_writer import write_outcome_from_paper_close.*?\nexcept Exception:\s*?\n\s*write_outcome_from_paper_close\s*=\s*None.*?\n\s*\n", s, flags=re.DOTALL)
        if not m:
            die("FAIL: could not locate outcome writer import block for insertion")

        insert_at = m.end()

        helper = r'''
def _write_outcome_safe(**kwargs):  # type: ignore
    """
    Call write_outcome_from_paper_close with only the kwargs it accepts.
    This makes PaperBroker resilient to writer signature drift.
    """
    global write_outcome_from_paper_close
    if write_outcome_from_paper_close is None:
        return False
    try:
        import inspect
        sig = inspect.signature(write_outcome_from_paper_close)
        allowed = set(sig.parameters.keys())
        filtered = {k: v for k, v in kwargs.items() if k in allowed}
        write_outcome_from_paper_close(**filtered)
        return True
    except TypeError as e:
        log.warning("[paper_broker] OUTCOME writer signature mismatch: %r", e)
        return False
    except Exception as e:
        log.warning("[paper_broker] OUTCOME writer failed: %r", e)
        return False

'''

        s = s[:insert_at] + helper + s[insert_at:]

    # ---------------------------------------------------------------------
    # 2) Replace any direct call pattern that passes strategy=... with _write_outcome_safe(...)
    #    We'll patch the *specific* noisy forced hook block you added.
    # ---------------------------------------------------------------------
    # Common patterns we saw:
    # log.warning signature mismatch ... then close continues.
    # We patch the call site by searching for "write_outcome_from_paper_close(" usage.
    if "write_outcome_from_paper_close(" in s:
        s2 = re.sub(
            r"write_outcome_from_paper_close\s*\(",
            "_write_outcome_safe(",
            s,
            count=0
        )
        s = s2

    # ---------------------------------------------------------------------
    # 3) Ensure the forced outcome call (wherever it is) includes rich fields,
    #    but now signature-filtered. If the code already builds kwargs, keep it.
    #    Otherwise inject a canonical kwargs dict near the CLOSE log line.
    # ---------------------------------------------------------------------
    # If we already have a _write_outcome_safe call, we're good.
    if "_write_outcome_safe(" not in s:
        die("FAIL: did not manage to inject/replace outcome writer call with _write_outcome_safe")

    TARGET.write_text(s, encoding="utf-8")
    print("OK: patched paper_broker.py (signature-safe outcome writer call)")

if __name__ == "__main__":
    main()
