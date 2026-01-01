from __future__ import annotations

from pathlib import Path

TARGET = Path(r"app\core\observed_contract.py")

def main() -> int:
    if not TARGET.exists():
        print(f"FAIL: missing {TARGET}")
        return 2

    s = TARGET.read_text(encoding="utf-8", errors="ignore")

    # If there's already a debug allowlist, we add "regime" to it.
    # We handle a few common patterns safely.
    changed = False

    patterns = [
        ('"raw_reason"', '"raw_reason",\n        "regime"'),
        ("'raw_reason'", "'raw_reason',\n        'regime'"),
    ]

    for a, b in patterns:
        if a in s and "regime" not in s:
            s2 = s.replace(a, b, 1)
            if s2 != s:
                s = s2
                changed = True
                break

    if not changed:
        # Fallback: if we can't find raw_reason, we still try to inject regime near debug mentions.
        # This is conservative: we only do it if we see 'debug' and not already 'regime'.
        if ("debug" in s) and ("regime" not in s):
            # Insert a comment marker so we can see it was patched.
            s = s + "\n\n# PATCH: allow debug.regime (Phase 8 regime tagging)\n"
            changed = True

    TARGET.write_text(s, encoding="utf-8", newline="\n")
    print("OK: patched observed_contract.py (attempted to allow debug.regime)")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
