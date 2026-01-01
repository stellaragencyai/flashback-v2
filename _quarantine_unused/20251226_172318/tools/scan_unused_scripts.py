import ast, os, shutil, sys, json
from pathlib import Path
from collections import defaultdict
ROOT = Path(r"C:\flashback").resolve()
QUARANTINE = Path(r"C:\flashback\_quarantine_unused\20251226_172318").resolve()
IGNORE_DIRS = {
    ".git", "__pycache__", "_quarantine_unused", "venv", ".venv"
}
def iter_py_files():
    for p in ROOT.rglob("*.py"):
        if any(part in IGNORE_DIRS for part in p.parts):
            continue
        yield p
def module_name(path: Path):
    try:
        return ".".join(path.relative_to(ROOT).with_suffix("").parts)
    except Exception:
        return None
imports = defaultdict(set)
all_files = {}
reverse_imports = defaultdict(set)
for f in iter_py_files():
    mod = module_name(f)
    if not mod:
        continue
    all_files[mod] = f
    try:
        tree = ast.parse(f.read_text(encoding="utf-8"))
    except Exception:
        continue
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for n in node.names:
                imports[mod].add(n.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                imports[mod].add(node.module)
for src, targets in imports.items():
    for t in targets:
        reverse_imports[t].add(src)
unused = []
reason = {}
for mod, path in all_files.items():
    fname = path.name.lower()
    if any(x in fname for x in ["bak_", "_bak", "before_", "_before", "broken_", "_broken"]):
        unused.append(mod)
        reason[mod] = "backup_or_patch_file"
        continue
    if mod not in reverse_imports:
        if "orchestrator" in mod or "supervisor" in mod:
            continue
        if path.name.startswith("__"):
            continue
        unused.append(mod)
        reason[mod] = "never_imported"
report = {
    "root": str(ROOT),
    "quarantine": str(QUARANTINE),
    "unused_count": len(unused),
    "unused": {m: str(all_files[m]) for m in unused},
    "reason": reason,
}
QUARANTINE.mkdir(parents=True, exist_ok=True)
for m in unused:
    src = all_files[m]
    dst = QUARANTINE / src.relative_to(ROOT)
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(src), str(dst))
(Path(QUARANTINE) / "unused_report.json").write_text(
    json.dumps(report, indent=2), encoding="utf-8"
)
print("=== UNUSED SCRIPT SCAN COMPLETE ===")
print(f"Quarantined: {len(unused)} files")
print(f"Report: {QUARANTINE / 'unused_report.json'}")
