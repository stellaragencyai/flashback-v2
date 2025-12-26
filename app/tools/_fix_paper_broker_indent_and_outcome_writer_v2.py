from __future__ import annotations

from pathlib import Path
import re
import inspect
from typing import Any, Dict

TARGET = Path(r"app\sim\paper_broker.py")

def die(msg: str) -> None:
    raise SystemExit(msg)

def main() -> None:
    if not TARGET.exists():
        die(f"FAIL: missing {TARGET}")

    raw = TARGET.read_text(encoding="utf-8", errors="ignore")

    # 1) Normalize indentation: tabs -> 4 spaces (eliminates most "unindent mismatch" landmines)
    raw = raw.replace("\t", "    ")

    lines = raw.splitlines()

    # 2) Remove any previously injected broken outcomes blocks inside PaperBroker._close_position
    # We remove from marker "# --- outcomes.v1" through the next blank line after the writer call rewrite.
    out_lines = []
    in_rm = False
    rm_mode = None
    rm_done = False

    for i, line in enumerate(lines):
        if not rm_done and re.search(r"#\s*---\s*outcomes\.v1", line):
            in_rm = True
            rm_mode = "outcomes_v1_block"
            continue

        if in_rm and rm_mode == "outcomes_v1_block":
            # End removal after we pass the rewritten call line (ok = _write_outcome_safe(...) or write_outcome_from_paper_close(**...))
            if re.search(r"\bok\s*=\s*_write_outcome_safe\(", line) or re.search(r"\bwrite_outcome_from_paper_close\(\*\*", line):
                # Keep consuming until a blank line after this call (or next non-indented section)
                rm_mode = "after_call"
                continue
            else:
                continue

        if in_rm and rm_mode == "after_call":
            if line.strip() == "":
                in_rm = False
                rm_done = True
            continue

        out_lines.append(line)

    text = "\n".join(out_lines) + "\n"

    # 3) Ensure we have a safe writer helper at module level (idempotent)
    if "_write_outcome_safe(" not in text:
        helper = r'''
# ---------------------------------------------------------------------------
# outcomes.v1 safe writer (signature-filtered, fail-soft)
# ---------------------------------------------------------------------------
def _write_outcome_safe(**kwargs: Any) -> bool:
    """
    Calls write_outcome_from_paper_close with only the kwargs it accepts.
    Prevents signature mismatch across versions.
    """
    global write_outcome_from_paper_close
    fn = write_outcome_from_paper_close
    if fn is None:
        return False
    try:
        sig = inspect.signature(fn)
        accepted = set(sig.parameters.keys())
        filtered = {k: v for k, v in kwargs.items() if k in accepted}
        fn(**filtered)
        return True
    except Exception as e:
        try:
            log.warning("[paper_broker] OUTCOME writer failed: %r", e)
        except Exception:
            pass
        return False
'''
        # Insert right after the existing "write_outcome_from_paper_close = None" block
        m = re.search(r"write_outcome_from_paper_close\s*=\s*None.*?\n", text)
        if not m:
            # fallback: insert after "log = get_logger"
            m2 = re.search(r"^log\s*=\s*get_logger.*?\n", text, flags=re.MULTILINE)
            if not m2:
                die("FAIL: could not find insertion anchor for outcome helper")
            insert_at = m2.end()
        else:
            insert_at = m.end()

        text = text[:insert_at] + helper + text[insert_at:]

    # 4) Inject a clean outcome write block INSIDE PaperBroker._close_position with correct indentation
    # We'll insert after pos.exit_reason assignment (or after pos.exit_price) if present.
    tlines = text.splitlines()

    # Find class PaperBroker and def _close_position
    cls_i = None
    for i, l in enumerate(tlines):
        if re.match(r"^\s*class\s+PaperBroker\s*:", l):
            cls_i = i
            break
    if cls_i is None:
        die("FAIL: could not find class PaperBroker")

    close_i = None
    for i in range(cls_i, len(tlines)):
        if re.match(r"^\s+def\s+_close_position\s*\(", tlines[i]):
            close_i = i
            break
    if close_i is None:
        die("FAIL: could not find def _close_position")

    method_indent = len(tlines[close_i]) - len(tlines[close_i].lstrip(" "))
    body_indent = " " * (method_indent + 4)

    # Determine method end
    end_i = None
    for j in range(close_i + 1, len(tlines)):
        if re.match(r"^\s*def\s+", tlines[j]) and (len(tlines[j]) - len(tlines[j].lstrip(" ")) == method_indent):
            end_i = j
            break
    if end_i is None:
        end_i = len(tlines)

    block = tlines[close_i:end_i]

    # If we already have our clean injection (idempotent), skip reinject
    if any("OUTCOME_REQUIRED_V1" in l for l in block):
        TARGET.write_text("\n".join(tlines) + "\n", encoding="utf-8")
        print("OK: paper_broker.py normalized (tabs->spaces); outcome writer already present.")
        return

    # Find insertion point inside _close_position
    ins_k = None
    for k in range(len(block)):
        if re.search(r"pos\.exit_reason\s*=", block[k]):
            ins_k = k + 1
            break
    if ins_k is None:
        for k in range(len(block)):
            if re.search(r"pos\.exit_price\s*=", block[k]):
                ins_k = k + 1
                break
    if ins_k is None:
        # fallback: after pnl calc (pnl = ...)
        for k in range(len(block)):
            if re.search(r"^\s+pnl\s*=\s*\(", block[k]):
                ins_k = k + 1
                break
    if ins_k is None:
        die("FAIL: could not find stable insertion point in _close_position")

    inject = [
        f"{body_indent}# OUTCOME_REQUIRED_V1 (signature-filtered write, fail-soft)",
        f"{body_indent}try:",
        f"{body_indent}    _outcome = {{",
        f"{body_indent}        'trade_id': str(pos.trade_id),",
        f"{body_indent}        'symbol': str(pos.symbol),",
        f"{body_indent}        'account_label': str(self._state.account_label),",
        f"{body_indent}        'strategy': str(self._state.strategy_name),",
        f"{body_indent}        'entry_side': 'Buy' if pos.side == 'long' else 'Sell',",
        f"{body_indent}        'entry_qty': float(pos.size),",
        f"{body_indent}        'entry_px': float(pos.entry_price),",
        f"{body_indent}        'opened_ts_ms': int(pos.opened_ms),",
        f"{body_indent}        'exit_px': float(exit_price),",
        f"{body_indent}        'exit_qty': float(pos.size),",
        f"{body_indent}        'closed_ts_ms': int(pos.closed_ms or _now_ms()),",
        f"{body_indent}        'fees_usd': 0.0,",
        f"{body_indent}        'mode': 'PAPER',",
        f"{body_indent}        'close_reason': str(exit_reason),",
        f"{body_indent}        'pnl_usd': float(pnl),",
        f"{body_indent}        'r_multiple': r_mult,",
        f"{body_indent}        'risk_usd': float(pos.risk_usd),",
        f"{body_indent}    }}",
        f"{body_indent}    _ok = _write_outcome_safe(**_outcome)",
        f"{body_indent}    if _ok:",
        f"{body_indent}        log.info('[paper_broker] ✅ outcomes.v1 write ok trade_id=%s', pos.trade_id)",
        f"{body_indent}except Exception as e:",
        f"{body_indent}    log.warning('[paper_broker] OUTCOME write wrapper failed: %r', e)",
        "",
    ]

    new_block = block[:ins_k] + inject + block[ins_k:]
    new_lines = tlines[:close_i] + new_block + tlines[end_i:]

    TARGET.write_text("\n".join(new_lines) + "\n", encoding="utf-8")
    print("OK: paper_broker.py fixed (tabs->spaces; broken injections removed; safe outcomes.v1 writer installed)")

if __name__ == "__main__":
    main()
