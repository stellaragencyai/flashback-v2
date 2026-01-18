from pathlib import Path
import re

p = Path("app/sim/paper_broker.py")
L = p.read_text(encoding="utf-8", errors="ignore").splitlines(True)

def die(msg: str):
    raise SystemExit("FAIL: " + msg)

text = "".join(L)

# ------------------------------------------------------------
# 1) REMOVE any existing helper block (wherever it landed)
#    We remove from "def _maybe_publish_outcome_record(" up to
#    the next top-level def/class or EOF.
# ------------------------------------------------------------
pat_start = re.compile(r"^def _maybe_publish_outcome_record\(", re.M)
m = pat_start.search(text)
if m:
    start = m.start()
    # Find next top-level boundary after start (def/class at col 0), skipping the helper itself
    m2 = re.search(r"^(def\s+\w+\(|class\s+\w+)", text[m.end():], flags=re.M)
    end = (m.end() + m2.start()) if m2 else len(text)
    text = text[:start] + text[end:]

# ------------------------------------------------------------
# 2) INSERT a clean helper at a SAFE top-level location:
#    right after the import block near the top of the file.
# ------------------------------------------------------------
lines = text.splitlines(True)

# Find insertion point: after last contiguous import/from block at top
ins = None
seen_imports = False
for i, ln in enumerate(lines):
    s = ln.strip()
    if s.startswith(("import ", "from ")):
        seen_imports = True
        continue
    if seen_imports:
        # allow blank lines and comments directly after imports
        if s == "" or s.startswith("#"):
            continue
        ins = i
        break
if ins is None:
    # fallback: very top
    ins = 0

helper = r'''
def _maybe_publish_outcome_record(
    *,
    account_label: str,
    strategy: str,
    trade_id: str,
    symbol: str,
    side: Side,
    qty: float,
    entry_px: float,
    opened_ms: int,
    exit_px: float,
    closed_ms: int,
    fees_usd: float,
    mode: str,
    close_reason: str,
    pnl_usd: float,
    r_multiple: Optional[float],
    setup_type: Optional[str],
    timeframe: Optional[str],
    ai_profile: Optional[str],
) -> bool:
    """Publish canonical outcome_record into ai_events_spine (single-writer)."""
    try:
        from app.ai.ai_events_spine import build_outcome_record, publish_ai_event
        evt = build_outcome_record(
            trade_id=str(trade_id),
            symbol=str(symbol),
            account_label=str(account_label),
            strategy=str(strategy),
            pnl_usd=float(pnl_usd),
            r_multiple=float(r_multiple) if r_multiple is not None else None,
            win=(float(pnl_usd) > 0.0),
            exit_reason=str(close_reason),
            timeframe=timeframe,
            extra={
                "mode": str(mode),
                "entry_side": "Buy" if side == "long" else "Sell",
                "entry_qty": float(qty),
                "entry_px": float(entry_px),
                "opened_ts_ms": int(opened_ms),
                "exit_side": "Sell" if side == "long" else "Buy",
                "exit_qty": float(qty),
                "exit_px": float(exit_px),
                "closed_ts_ms": int(closed_ms),
                "fees_usd": float(fees_usd),
                "setup_type": setup_type,
                "timeframe": timeframe,
                "ai_profile": ai_profile,
                "close_reason": str(close_reason),
            },
        )
        publish_ai_event(evt)
        return True
    except Exception:
        return False
'''

# Ensure there is a blank line before/after for cleanliness
insert_chunk = []
if ins > 0 and not lines[ins-1].endswith("\n"):
    lines[ins-1] = lines[ins-1] + "\n"
insert_chunk.append("\n")
insert_chunk.append(helper)
insert_chunk.append("\n")

new_text = "".join(lines[:ins] + insert_chunk + lines[ins:])
p.write_text(new_text, encoding="utf-8")

print(f"OK: helper rehomed to top-level after imports in {p}")
