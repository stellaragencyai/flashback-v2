from __future__ import annotations

from pathlib import Path
import re

TARGET = Path(r"app\sim\paper_broker.py")

def die(msg: str) -> None:
    raise SystemExit(msg)

def find_line_idx(lines, pattern, start=0):
    rx = re.compile(pattern)
    for i in range(start, len(lines)):
        if rx.search(lines[i]):
            return i
    return None

def main() -> None:
    if not TARGET.exists():
        die(f"FAIL: missing {TARGET}")

    text = TARGET.read_text(encoding="utf-8", errors="ignore")
    lines = text.splitlines()

    cls_i = find_line_idx(lines, r"^\s*class\s+PaperBroker\s*:")
    if cls_i is None:
        die("FAIL: could not find class PaperBroker")

    close_i = find_line_idx(lines, r"^\s+def\s+_close_position\s*\(", start=cls_i)
    if close_i is None:
        die("FAIL: could not find PaperBroker._close_position()")

    # Determine method indent (number of leading spaces on def line)
    def_line = lines[close_i]
    indent = len(def_line) - len(def_line.lstrip(" "))
    if indent <= 0:
        die("FAIL: _close_position indent not detected (unexpected formatting)")

    # Find end of method: next def at same indent level
    end_i = None
    for j in range(close_i + 1, len(lines)):
        lj = lines[j]
        if re.match(r"^\s*def\s+", lj) and (len(lj) - len(lj.lstrip(" ")) == indent):
            end_i = j
            break
    if end_i is None:
        end_i = len(lines)

    block = lines[close_i:end_i]
    block_text = "\n".join(block)

    if "_outcome_required" in block_text:
        die("SKIP: _outcome_required already present in _close_position (patch already applied)")

    # Find first outcome writer call inside block
    call_idx = None
    call_kind = None
    for k in range(len(block)):
        if "_write_outcome_safe(" in block[k]:
            call_idx = k
            call_kind = "_write_outcome_safe"
            break
    if call_idx is None:
        for k in range(len(block)):
            if "write_outcome_from_paper_close(" in block[k]:
                call_idx = k
                call_kind = "write_outcome_from_paper_close"
                break
    if call_idx is None:
        die("FAIL: could not find outcome writer call inside _close_position (no _write_outcome_safe / writer call)")

    # Inject required kwargs dict ABOVE that call
    inj_indent = " " * (indent + 4)  # inside method body
    inject = [
        f"{inj_indent}# --- outcomes.v1 (required schema) ---",
        f"{inj_indent}# The outcome writer requires these keyword-only fields.",
        f"{inj_indent}# We always provide them; the safe-writer can still filter by signature.",
        f"{inj_indent}_outcome_required = {{",
        f"{inj_indent}    \"trade_id\": str(pos.trade_id),",
        f"{inj_indent}    \"symbol\": str(pos.symbol),",
        f"{inj_indent}    \"account_label\": str(self._state.account_label),",
        f"{inj_indent}    \"strategy\": str(self._state.strategy_name),",
        f"{inj_indent}",
        f"{inj_indent}    \"entry_side\": \"Buy\" if pos.side == \"long\" else \"Sell\",",
        f"{inj_indent}    \"entry_qty\": float(pos.size),",
        f"{inj_indent}    \"entry_px\": float(pos.entry_price),",
        f"{inj_indent}    \"opened_ts_ms\": int(pos.opened_ms),",
        f"{inj_indent}",
        f"{inj_indent}    \"exit_px\": float(exit_price),",
        f"{inj_indent}    \"exit_qty\": float(pos.size),",
        f"{inj_indent}    \"closed_ts_ms\": int(pos.closed_ms or _now_ms()),",
        f"{inj_indent}",
        f"{inj_indent}    \"fees_usd\": 0.0,",
        f"{inj_indent}    \"mode\": \"PAPER\",",
        f"{inj_indent}    \"close_reason\": str(exit_reason),",
        f"{inj_indent}",
        f"{inj_indent}    # Extras (writer may ignore via signature-filter)",
        f"{inj_indent}    \"pnl_usd\": float(pnl),",
        f"{inj_indent}    \"r_multiple\": r_mult,",
        f"{inj_indent}    \"risk_usd\": float(pos.risk_usd),",
        f"{inj_indent}    \"win\": (r_mult is not None and r_mult > 0),",
        f"{inj_indent}    \"equity_before\": float(equity_before),",
        f"{inj_indent}    \"equity_after\": float(equity_after),",
        f"{inj_indent}}}",
        "",
    ]

    new_block = block[:call_idx] + inject + block[call_idx:]

    # Force the call line to use **_outcome_required.
    # We only rewrite the FIRST occurrence.
    rewritten = False
    for k in range(len(new_block)):
        line = new_block[k]
        if not rewritten and call_kind == "_write_outcome_safe" and "_write_outcome_safe(" in line:
            new_block[k] = re.sub(r"_write_outcome_safe\s*\(.*", f"{inj_indent}ok = _write_outcome_safe(**_outcome_required)", line)
            rewritten = True
        elif not rewritten and call_kind == "write_outcome_from_paper_close" and "write_outcome_from_paper_close(" in line:
            new_block[k] = re.sub(r"write_outcome_from_paper_close\s*\(.*", f"{inj_indent}write_outcome_from_paper_close(**_outcome_required)", line)
            rewritten = True

    if not rewritten:
        die("FAIL: found writer call but could not rewrite it safely")

    # Write back
    out_lines = lines[:close_i] + new_block + lines[end_i:]
    TARGET.write_text("\n".join(out_lines) + "\n", encoding="utf-8")
    print("OK: patched paper_broker.py (_close_position now passes required outcomes.v1 kwargs)")

if __name__ == "__main__":
    main()
