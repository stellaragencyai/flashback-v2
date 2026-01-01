from pathlib import Path
import re

p = Path(r"config\fleet_manifest.yaml")
txt = p.read_text(encoding="utf-8", errors="replace")

try:
    import yaml  # type: ignore
    yaml.safe_load(txt)
    print("OK: YAML_PARSE (no error)")
except Exception as e:
    print(f"YAML_ERROR={e!r}")

    # Try to extract line/column from common PyYAML error formats
    msg = str(e)
    m = re.search(r"line\s+(\d+),\s+column\s+(\d+)", msg)
    if not m:
        # ScannerError uses "in \"<unicode string>\", line X, column Y"
        m = re.search(r"line\s+(\d+),\s+column\s+(\d+)", msg)
    if m:
        line = int(m.group(1))
        col = int(m.group(2))
        L = txt.splitlines()
        a = max(1, line - 20)
        b = min(len(L), line + 20)
        print(f"CONTEXT line={line} col={col}")
        for n in range(a, b + 1):
            s = L[n - 1]
            ind = len(s) - len(s.lstrip(" "))
            show = s.replace("\t", "\\t")
            marker = ""
            if n == line:
                marker = "  <=== HERE"
            print(f"{n:04d} indent={ind:02d} | {show!r}{marker}")
    else:
        print("NOTE: Could not extract line/col from error string. We'll rebuild fleet block anyway.")
