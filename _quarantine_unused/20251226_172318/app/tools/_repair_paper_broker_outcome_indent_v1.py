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

    lines = TARGET.read_text(encoding="utf-8", errors="ignore").splitlines()

    cls_i = find_line_idx(lines, r"^\s*class\s+PaperBroker\s*:")
    if cls_i is None:
        die("FAIL: could not find class PaperBroker")

    close_i = find_line_idx(lines, r"^\s+def\s+_close_position\s*\(", start=cls_i)
    if close_i is None:
        die("FAIL: could not find PaperBroker._close_position()")

    # Method indent level
    def_line = lines[close_i]
    method_indent = len(def_line) - len(def_line.lstrip(" "))
    if method_indent <= 0:
        die("FAIL: unexpected _close_position indent")

    # End of method: next def at same indent
    end_i = None
    for j in range(close_i + 1, len(lines)):
        lj = lines[j]
        if re.match(r"^\s*def\s+", lj) and (len(lj) - len(lj.lstrip(" ")) == method_indent):
            end_i = j
            break
    if end_i is None:
        end_i = len(lines)

    block = lines[close_i:end_i]

    # 1) Remove any previously injected outcomes.v1 block
    start_rm = None
    end_rm = None
    for k in range(len(block)):
        if re.search(r"#\s*---\s*outcomes\.v1\s*\(required schema\)\s*---", block[k]):
            start_rm = k
            break
    if start_rm is not None:
        # remove through the closing "}" of _outcome_required plus trailing blank line
        brace_depth = 0
        for k in range(start_rm, len(block)):
            if "_outcome_required" in block[k] and "{" in block[k]:
                brace_depth = 1
            elif brace_depth > 0:
                brace_depth += block[k].count("{")
                brace_depth -= block[k].count("}")
                if brace_depth <= 0:
                    end_rm = k
                    break
        if end_rm is None:
            # fallback: remove 60 lines max
            end_rm = min(start_rm + 60, len(block) - 1)

        # also remove one trailing blank line if present
        end_rm2 = end_rm
        if end_rm2 + 1 < len(block) and block[end_rm2 + 1].strip() == "":
            end_rm2 += 1

        block = block[:start_rm] + block[end_rm2 + 1:]

    # 2) Find the outcome writer call line (prefer safe writer)
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
        die("FAIL: could not find outcome writer call inside _close_position")

    # 3) Use indent of the CALL LINE (handles try/except indentation correctly)
    call_line = block[call_idx]
    call_indent_n = len(call_line) - len(call_line.lstrip(" "))
    inj_indent = " " * call_indent_n

    inject = [
        f"{inj_indent}# --- outcomes.v1 (required schema) ---",
        f"{inj_indent}# The outcome writer requires these keyword-only fields.",
        f"{inj_indent}# We always provide them; safe-writer can still filter by signature.",
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

    # 4) Rewrite the FIRST writer call line to use **_outcome_required
    rewritten = False
    for k in range(len(new_block)):
        if not rewritten and call_kind == "_write_outcome_safe" and "_write_outcome_safe(" in new_block[k]:
            new_block[k] = f"{inj_indent}ok = _write_outcome_safe(**_outcome_required)"
            rewritten = True
        elif not rewritten and call_kind == "write_outcome_from_paper_close" and "write_outcome_from_paper_close(" in new_block[k]:
            new_block[k] = f"{inj_indent}write_outcome_from_paper_close(**_outcome_required)"
            rewritten = True

    if not rewritten:
        die("FAIL: found writer call but could not rewrite it")

    out_lines = lines[:close_i] + new_block + lines[end_i:]
    TARGET.write_text("\n".join(out_lines) + "\n", encoding="utf-8")
    print("OK: repaired paper_broker.py (outcome kwargs injected with correct indentation)")

if __name__ == "__main__":
    main()
