from __future__ import annotations

from pathlib import Path
import re

TARGET = Path(r"app\sim\paper_broker.py")

def die(msg: str) -> None:
    raise SystemExit(msg)

def main() -> None:
    if not TARGET.exists():
        die(f"FAIL: missing {TARGET}")

    s = TARGET.read_text(encoding="utf-8", errors="ignore")

    # Find the _close_position function body (we’ll inject required outcome kwargs in there)
    m = re.search(r"^def\s+_close_position\s*\(\s*self\s*,[\s\S]*?^\s*def\s+update_price\s*\(",
                  s, flags=re.MULTILINE)
    if not m:
        die("FAIL: could not locate _close_position() block")

    block = m.group(0)

    # We expect an outcomes write attempt call in _close_position
    if "_write_outcome_safe" not in block and "write_outcome_from_paper_close" not in block:
        die("FAIL: _close_position does not appear to call outcome writer; cannot patch safely")

    # Inject a required kwargs dict near the close logic.
    # We patch by inserting a block right before the first outcome write attempt inside _close_position.
    # We look for a line containing '_write_outcome_safe(' and insert a standardized kwargs build above it.
    needle = re.search(r"^\s*(?:ok\s*=\s*)?_write_outcome_safe\s*\(", block, flags=re.MULTILINE)
    if not needle:
        # fallback: direct writer call
        needle = re.search(r"^\s*(?:ok\s*=\s*)?write_outcome_from_paper_close\s*\(", block, flags=re.MULTILINE)
    if not needle:
        die("FAIL: could not find outcome writer call site inside _close_position")

    insert_at = needle.start()

    inject = """
        # --- outcomes.v1 (required schema) ---
        # The outcome writer requires these keyword-only fields.
        # We always provide them; _write_outcome_safe will still filter by signature.
        _outcome_required = {
            "trade_id": str(pos.trade_id),
            "symbol": str(pos.symbol),
            "account_label": str(self._state.account_label),
            "strategy": str(self._state.strategy_name),

            "entry_side": "Buy" if pos.side == "long" else "Sell",
            "entry_qty": float(pos.size),
            "entry_px": float(pos.entry_price),
            "opened_ts_ms": int(pos.opened_ms),

            "exit_px": float(exit_price),
            "exit_qty": float(pos.size),
            "closed_ts_ms": int(pos.closed_ms or _now_ms()),

            "fees_usd": 0.0,
            "mode": "PAPER",
            "close_reason": str(exit_reason),

            # Extras (writer may ignore via signature-filter)
            "pnl_usd": float(pnl),
            "r_multiple": r_mult,
            "risk_usd": float(pos.risk_usd),
            "win": (r_mult is not None and r_mult > 0),
            "equity_before": float(equity_before),
            "equity_after": float(equity_after),
        }
"""

    # Avoid double-injecting if you run patch twice
    if "_outcome_required" in block:
        die("SKIP: _outcome_required already present; patch already applied")

    # Insert the inject block
    new_block = block[:insert_at] + inject + block[insert_at:]

    # Now ensure the writer call uses _outcome_required as kwargs
    # Replace the first call arguments with **_outcome_required if it already passed kwargs
    new_block = re.sub(
        r"(^\s*(?:ok\s*=\s*)?_write_outcome_safe\s*\()\s*.*?\)",
        r"\1**_outcome_required)",
        new_block,
        count=1,
        flags=re.MULTILINE | re.DOTALL
    )
    new_block = re.sub(
        r"(^\s*(?:ok\s*=\s*)?write_outcome_from_paper_close\s*\()\s*.*?\)",
        r"\1**_outcome_required)",
        new_block,
        count=1,
        flags=re.MULTILINE | re.DOTALL
    )

    out = s[:m.start()] + new_block + s[m.end():]
    TARGET.write_text(out, encoding="utf-8")
    print("OK: patched paper_broker.py (passes required outcome writer kwargs on close)")

if __name__ == "__main__":
    main()
