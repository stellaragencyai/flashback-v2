from __future__ import annotations

from pathlib import Path

FILES = [
    Path(r"app\bots\signal_engine.py"),
    Path(r"app\core\observed_contract.py"),
]

BOM = "\ufeff"

def strip_bom(p: Path) -> bool:
    if not p.exists():
        print(f"SKIP: missing {p}")
        return False
    s = p.read_text(encoding="utf-8", errors="ignore")
    if s.startswith(BOM):
        s2 = s.lstrip(BOM)
        p.write_text(s2, encoding="utf-8", newline="\n")
        print(f"OK: stripped BOM -> {p}")
        return True
    print(f"OK: no BOM -> {p}")
    return False

def main() -> int:
    changed = 0
    for p in FILES:
        if strip_bom(p):
            changed += 1

    # Compile check
    import py_compile
    for p in FILES:
        py_compile.compile(str(p), doraise=True)
    print("PASS: compiled (post-BOM-strip)")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
