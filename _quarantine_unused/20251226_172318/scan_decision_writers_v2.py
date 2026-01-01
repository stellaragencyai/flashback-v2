from __future__ import annotations
import ast
from pathlib import Path

ROOT = Path("app")
TARGET = "ai_decisions.jsonl"

def is_python_file(p: Path) -> bool:
    if p.suffix.lower() != ".py":
        return False
    name = p.name.lower()
    # skip backups and junk copies
    if ".bak" in name or "backup" in name or name.endswith(".py~"):
        return False
    return True

def extract_str(node):
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None

class WriterFinder(ast.NodeVisitor):
    def __init__(self):
        self.hits = []

    def visit_Call(self, node: ast.Call):
        # open("...ai_decisions.jsonl...", "a"/"ab"/"wb"/etc)
        fn = node.func
        fn_name = None
        if isinstance(fn, ast.Name):
            fn_name = fn.id
        elif isinstance(fn, ast.Attribute):
            fn_name = fn.attr

        # Detect open(...) with TARGET in path literal
        if fn_name == "open":
            if node.args:
                s0 = extract_str(node.args[0])
                if s0 and TARGET in s0:
                    self.hits.append(("open()", node.lineno, ast.get_source_segment(self.src, node) or "open(...)"))

        # Detect Path(...).open("a"/"ab") style if arg contains target literal upstream
        if fn_name == "open" and isinstance(fn, ast.Attribute):
            # This catches path.open("ab") but we only count it if the variable name suggests decisions
            # (DECISIONS_PATH / AI_DECISIONS_PATH / decisions_path)
            if isinstance(fn.value, ast.Name):
                var = fn.value.id.lower()
                if "decision" in var and any(mode in (extract_str(a) or "") for a in node.args for mode in ("a","ab","wb","w")):
                    self.hits.append((f"{fn.value.id}.open()", node.lineno, ast.get_source_segment(self.src, node) or "path.open(...)"))

        # Detect os.open(str(path), O_APPEND...) and similar, if TARGET literal appears in the call
        if fn_name == "open" and isinstance(fn, ast.Attribute):
            # os.open(...)
            if isinstance(fn.value, ast.Name) and fn.value.id == "os":
                for a in node.args:
                    s = extract_str(a)
                    if s and TARGET in s:
                        self.hits.append(("os.open()", node.lineno, ast.get_source_segment(self.src, node) or "os.open(...)"))

        self.generic_visit(node)

def scan_file(path: Path):
    src = path.read_text(encoding="utf-8", errors="ignore")
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return []
    vf = WriterFinder()
    vf.src = src
    vf.visit(tree)
    return vf.hits

bad = []
for f in ROOT.rglob("*.py"):
    if not is_python_file(f):
        continue
    hits = scan_file(f)
    # Only report files that mention ai_decisions.jsonl at all (reduces noise)
    if hits:
        # extra filter: ensure file text contains target
        s = f.read_text(encoding="utf-8", errors="ignore")
        if TARGET in s:
            bad.append((str(f), hits))

print("DECISION_WRITERS_V2:", len(bad))
for file, hits in bad:
    print(file)
    for kind, lineno, snippet in hits:
        print(f"  {lineno:04d} [{kind}] {snippet.strip()}")
