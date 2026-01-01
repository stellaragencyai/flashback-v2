from __future__ import annotations

from pathlib import Path
import ast
import json
import re
from collections import defaultdict

ROOT = Path("app")
CONFIG = Path("config")

# Your canonical runtime entrypoints (these are the only ones that matter)
ENTRYPOINT_MODULES = [
    "app.bots.supervisor",
    "app.bots.executor_v2",
    "app.bots.tp_sl_manager",
    "app.bots.risk_daemon",
    "app.bots.ws_switchboard",
    "app.ai.ai_decision_outcome_linker",
    "app.bots.trade_outcome_recorder",
]

# Files we will scan for string-based dynamic imports and module references
DYNAMIC_IMPORT_HINTS = [
    re.compile(r"importlib\.import_module\(\s*['\"]([^'\"]+)['\"]\s*\)"),
    re.compile(r"__import__\(\s*['\"]([^'\"]+)['\"]\s*\)"),
    re.compile(r"_import_first\([^)]*\[\s*([^\]]+)\]"),
    re.compile(r"WorkerSpec\(\s*['\"]([^'\"]+)['\"]\s*,"),
    re.compile(r"['\"](app\.[a-zA-Z0-9_\.]+)['\"]"),
]

# Config files that often contain module names or feature toggles
CONFIG_GLOBS = [
    "bots.yaml",
    "orchestrator.yaml",
    "fleet_manifest.yaml",
    "sub_stack_manifest.yaml",
    "governance.yaml",
    "ai_profiles.yaml",
]

def mod_to_path(mod: str) -> str:
    return str(Path(*mod.split(".")) .with_suffix(".py")).replace("/", "\\")

def path_to_mod(p: Path) -> str:
    rel = p.relative_to(Path("."))
    s = str(rel).replace("/", "\\")
    if not s.lower().endswith(".py"):
        return ""
    s = s[:-3]
    return s.replace("\\", ".")

def safe_read(p: Path) -> str:
    try:
        return p.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""

def add_keep_file(keep_files: set[str], p: Path):
    s = str(p).replace("/", "\\")
    if s.lower().startswith("app\\") and s.lower().endswith(".py"):
        keep_files.add(s)

def collect_imports_from_ast(py_path: Path) -> set[str]:
    txt = safe_read(py_path)
    mods = set()
    try:
        tree = ast.parse(txt)
    except Exception:
        return mods

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for n in node.names:
                if n.name.startswith("app."):
                    mods.add(n.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module and node.module.startswith("app."):
                mods.add(node.module)
    return mods

def collect_dynamic_module_strings(text: str) -> set[str]:
    found = set()
    for pat in DYNAMIC_IMPORT_HINTS:
        for m in pat.findall(text):
            if isinstance(m, tuple):
                # for list capture patterns
                for item in m:
                    if isinstance(item, str) and item.startswith("app."):
                        found.add(item)
            elif isinstance(m, str) and m.startswith("app."):
                found.add(m)

    # Special handling for _import_first list literals: ["app.x", "app.y"]
    if "_import_first" in text:
        for mm in re.findall(r"\[\s*([^\]]+)\]", text, flags=re.S):
            for s in re.findall(r"['\"](app\.[a-zA-Z0-9_\.]+)['\"]", mm):
                found.add(s)

    return found

def scan_config_for_modules() -> set[str]:
    mods = set()
    if not CONFIG.exists():
        return mods
    for name in CONFIG_GLOBS:
        p = CONFIG / name
        if not p.exists():
            continue
        t = safe_read(p)
        for s in re.findall(r"(app\.[a-zA-Z0-9_\.]+)", t):
            mods.add(s)
    return mods

def main():
    # Map module -> file path if exists
    all_py = list(ROOT.rglob("*.py"))
    mod_exists = {path_to_mod(p): p for p in all_py}

    keep_mods = set(ENTRYPOINT_MODULES)
    keep_files = set()

    # Seed keep files with entrypoints
    for m in ENTRYPOINT_MODULES:
        fp = Path(mod_to_path(m))
        if fp.exists():
            add_keep_file(keep_files, fp)

    # Also always keep package __init__.py under app (safe & tiny)
    for p in ROOT.rglob("__init__.py"):
        add_keep_file(keep_files, p)

    # Pull in module references from configs (module strings)
    keep_mods |= scan_config_for_modules()

    # Expand keep_mods via AST imports + dynamic string hints
    changed = True
    while changed:
        changed = False
        current = list(keep_mods)
        for mod in current:
            p = mod_exists.get(mod)
            if not p:
                continue
            add_keep_file(keep_files, p)

            txt = safe_read(p)

            # AST imports
            for dep in collect_imports_from_ast(p):
                if dep not in keep_mods:
                    keep_mods.add(dep)
                    changed = True

            # dynamic hints
            for dep in collect_dynamic_module_strings(txt):
                if dep not in keep_mods:
                    keep_mods.add(dep)
                    changed = True

    # Convert keep_mods -> keep_files where file exists
    for m in sorted(keep_mods):
        p = mod_exists.get(m)
        if p:
            add_keep_file(keep_files, p)

    # Build delete candidates: anything in app/*.py not in keep_files
    all_files = {str(p).replace("/", "\\") for p in all_py}
    delete_candidates = sorted(all_files - keep_files)

    # Output
    state = Path("state")
    state.mkdir(exist_ok=True)
    (state / "KEEP_FINAL.txt").write_text("\n".join(sorted(keep_files)) + "\n", encoding="utf-8")
    (state / "DELETE_CANDIDATES_FINAL.txt").write_text("\n".join(delete_candidates) + "\n", encoding="utf-8")

    print("ALL_PY=", len(all_files))
    print("KEEP_FINAL=", len(keep_files))
    print("DELETE_CANDIDATES_FINAL=", len(delete_candidates))
    print("WROTE state\\KEEP_FINAL.txt and state\\DELETE_CANDIDATES_FINAL.txt")

if __name__ == "__main__":
    main()
