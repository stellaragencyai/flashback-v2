import pathlib
import re

ROOTS = [
    "app/ai",
    "app/models",
    "app/training",
    "app/backtest",
]

GPU_IMPORT = "from app.ai.gpu_runtime import configure_gpu\nruntime, device = configure_gpu()\n"

def patch_file(path: pathlib.Path):
    txt = path.read_text(encoding="utf-8")

    if "configure_gpu()" in txt:
        return False

    uses_torch = "import torch" in txt
    uses_tf = "tensorflow" in txt

    if not (uses_torch or uses_tf):
        return False

    # Replace .cuda() with .to(device)
    txt = re.sub(r"\.cuda\(\)", ".to(device)", txt)

    # Inject runtime after imports
    lines = txt.splitlines()
    insert_at = 0
    for i, line in enumerate(lines):
        if line.startswith("import") or line.startswith("from"):
            insert_at = i + 1

    lines.insert(insert_at, GPU_IMPORT.rstrip())
    txt = "\n".join(lines)

    path.write_text(txt, encoding="utf-8")
    print(f"[PATCHED] {path}")
    return True


def main():
    patched = 0
    for root in ROOTS:
        base = pathlib.Path(root)
        if not base.exists():
            continue
        for py in base.rglob("*.py"):
            if patch_file(py):
                patched += 1

    print(f"\n✅ Total patched files: {patched}")

if __name__ == "__main__":
    main()
