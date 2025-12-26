from pathlib import Path

p = Path(r"app\bots\signal_engine.py")
s = p.read_text(encoding="utf-8", errors="ignore").splitlines()

needle = '    raw_list = data.get("result", {}).get("list", []) or []'
if needle not in s:
    raise SystemExit("PATCH FAILED: could not find raw_list line")

out = []
inserted = False
for line in s:
    out.append(line)
    if (not inserted) and line == needle:
        out.append("")
        out.append("    if not raw_list:")
        out.append("        # Empty list usually means symbol/category mismatch or delisted instrument")
        out.append("        raise RuntimeError(")
        out.append("            f\"Empty kline list for symbol={symbol} interval={interval} category={params.get('category')} retMsg={data.get('retMsg')}\"")
        out.append("        )")
        inserted = True

p.write_text("\n".join(out) + "\n", encoding="utf-8", newline="\n")
print("OK: added hard-fail for empty kline list")
