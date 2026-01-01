from __future__ import annotations

import ast
from pathlib import Path

APP = Path("app")
TARGET = "ai_decisions.jsonl"

def is_real_py(p: Path) -> bool:
    if p.suffix.lower() != ".py":
        return False
    name = p.name.lower()
    # quarantine common backups/noise
    if name.endswith(".bak.py") or ".bak_" in name or name.endswith(".py~"):
        return False
    return True

def _const_str(n):
    return n.value if isinstance(n, ast.Constant) and isinstance(n.value, str) else None

class HitFinder(ast.NodeVisitor):
    def __init__(self, src: str):
        self.src = src
        self.hits = []

    def visit_Call(self, node: ast.Call):
        fn = node.func

        # open("...ai_decisions.jsonl...", "a"/"ab"/"w"...)
        if isinstance(fn, ast.Name) and fn.id == "open":
            if node.args:
                s0 = _const_str(node.args[0])
                if s0 and TARGET in s0:
                    self.hits.append((node.lineno, "open()", ast.get_source_segment(self.src, node) or "open(...)"))

        # DECISIONS_PATH.open("ab") style: count ONLY if var name implies decisions
        if isinstance(fn, ast.Attribute) and fn.attr == "open":
            if isinstance(fn.value, ast.Name):
                var = fn.value.id.lower()
                if "decision" in var or "decisions" in var:
                    # if mode argument includes append/write
                    modes = []
                    for a in node.args:
                        sa = _const_str(a)
                        if sa:
                            modes.append(sa)
                    if any(m in ("a", "ab", "w", "wb") for m in modes):
                        self.hits.append((node.lineno, f"{fn.value.id}.open()", ast.get_source_segment(self.src, node) or "path.open(...)"))

        self.generic_visit(node)

def scan_one(file: Path):
    src = file.read_text(encoding="utf-8", errors="ignore")
    if TARGET not in src:
        return []
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return [(-1, "syntax_error", "ast.parse failed")]
    hf = HitFinder(src)
    hf.visit(tree)
    return hf.hits

bad = []
for f in APP.rglob("*.py"):
    if not is_real_py(f):
        continue
    hits = scan_one(f)
    if hits:
        bad.append((str(f), hits))

print("DECISION_WRITERS_AST:", len(bad))
for path, hits in bad:
    print(path)
    for ln, kind, snippet in hits:
        print(f"  {ln:04d} [{kind}] {snippet.strip()}")
