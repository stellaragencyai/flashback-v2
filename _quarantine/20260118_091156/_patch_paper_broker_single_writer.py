from pathlib import Path

p = Path("app/sim/paper_broker.py")
lines = p.read_text(encoding="utf-8", errors="ignore").splitlines(True)

def die(msg: str):
    raise SystemExit("FAIL: " + msg)

text0 = "".join(lines)
if "PAPER_BROKER_OUTCOME_V1_FALLBACK_WRITE" in text0 and "_maybe_publish_outcome_record" in text0:
    die("looks already patched")

# ----------------------------
# 1) Insert helper after setup_context warning block (anchor on known string)
# ----------------------------
needle_setup = "[paper_broker] Optional setup_context publish failed"
idx = None
for i, ln in enumerate(lines):
    if needle_setup in ln:
        idx = i
        break
if idx is None:
    die("could not find anchor warning line for setup_context publish")

# find end of containing block/function
end = None
for j in range(idx + 1, len(lines)):
    ln = lines[j]
    if ln.startswith("def ") or ln.startswith("class ") or (ln.strip() and not ln.startswith((" ", "\t", "#"))):
        end = j
        break
if end is None:
    end = len(lines)

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

lines = lines[:end] + [helper] + lines[end:]
text = "".join(lines)

# ----------------------------
# 2) Patch the function that contains the outcomes.v1 writer failed log
#    (anchor on that log line, then locate enclosing def block)
# ----------------------------
needle_fail = "outcomes.v1 writer failed"
fail_line = None
for i, ln in enumerate(text.splitlines(True)):
    if needle_fail in ln.lower():
        fail_line = i
        break
if fail_line is None:
    die("could not find anchor log: outcomes.v1 writer failed")

L = text.splitlines(True)

# find start of enclosing function def (scan upward)
start = None
for i in range(fail_line, -1, -1):
    if L[i].startswith("def "):
        start = i
        break
if start is None:
    die("could not locate enclosing def for outcomes.v1 writer failed")

# find end of that function def (next top-level def/class/non-indented)
end = None
for j in range(start + 1, len(L)):
    ln = L[j]
    if ln.startswith("def ") or ln.startswith("class ") or (ln.strip() and not ln.startswith((" ", "\t", "#"))):
        end = j
        break
if end is None:
    end = len(L)

func = "".join(L[start:end])

# We will replace everything from the last occurrence of "try:" that calls fn(**call_kwargs)
# down to the end of the function block.
needle_call = "fn(**call_kwargs)"
pos_call = func.rfind(needle_call)
if pos_call == -1:
    die("could not find fn(**call_kwargs) inside target function")

pos_try = func.rfind("\n    try:", 0, pos_call)
if pos_try == -1:
    # sometimes it's "\n\ttry:" or different indent; fall back to any "try:" before call
    pos_try = func.rfind("try:", 0, pos_call)
    if pos_try == -1:
        die("could not find try: before fn(**call_kwargs) inside target function")

head = func[:pos_try]

tail = r'''
    published = _maybe_publish_outcome_record(
        account_label=account_label,
        strategy=strategy,
        trade_id=trade_id,
        symbol=symbol,
        side=side,
        qty=qty,
        entry_px=entry_px,
        opened_ms=opened_ms,
        exit_px=exit_px,
        closed_ms=closed_ms,
        fees_usd=fees_usd,
        mode=mode,
        close_reason=close_reason,
        pnl_usd=pnl_usd,
        r_multiple=r_multiple,
        setup_type=setup_type,
        timeframe=timeframe,
        ai_profile=ai_profile,
    )
    if published:
        return

    # Optional fallback: legacy outcomes.v1 direct writer (OFF by default)
    # Enable only if you explicitly set:
    #   $env:PAPER_BROKER_OUTCOME_V1_FALLBACK_WRITE = "true"
    import os
    allow_fallback = str(os.getenv("PAPER_BROKER_OUTCOME_V1_FALLBACK_WRITE", "false")).strip().lower() in ("1","true","yes","y","on")
    if not allow_fallback:
        return

    try:
        fn(**call_kwargs)  # type: ignore
        log.info("[paper_broker] ✅ outcomes.v1 wrote trade_id=%s", trade_id)
    except Exception as e:
        log.warning("[paper_broker] outcomes.v1 writer failed: %r", e)
'''

new_func = head + tail

# splice function back into full file
new_text = "".join(L[:start]) + new_func + "".join(L[end:])

p.write_text(new_text, encoding="utf-8")
print("OK: paper_broker single-writer patch applied (anchor-based)")
