from __future__ import annotations
import re
from pathlib import Path

p = Path(r".\app\sim\paper_broker.py")
s = p.read_text(encoding="utf-8")

pattern = re.compile(
    r"def _maybe_write_outcome_v1_from_close\([\s\S]*?\n\n# ----------------------------\n# PaperBroker core",
    re.MULTILINE,
)

m = pattern.search(s)
if not m:
    raise SystemExit("Could not find _maybe_write_outcome_v1_from_close block to replace.")

replacement = r'''def _maybe_write_outcome_v1_from_close(
    *,
    account_label: str,
    strategy: str,
    trade_id: str,
    client_trade_id: Optional[str],
    source_trade_id: Optional[str],
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
) -> None:
    """
    Canonical outcome.v1 emission hook (PAPER only).

    IMPORTANT:
    - This function must be robust to different writer call signatures:
      (A) write_outcome_from_paper_close(payload=<dict>)
      (B) write_outcome_from_paper_close(**fields)
    """
    try:
        from app.ai.outcome_writer import write_outcome_from_paper_close  # type: ignore
    except Exception as e:
        log.warning("[paper_broker] outcomes.v1 writer import failed: %r", e)
        _append_jsonl_bytesafe(
            _OUTCOME_WRITE_FAIL_PATH,
            {"event_type": "outcome_writer_import_failed", "ts_ms": _now_ms(), "error": repr(e)},
        )
        return

    fn = write_outcome_from_paper_close  # type: ignore

    # Introspect signature to decide how to call writer
    try:
        sig = inspect.signature(fn)
        params = sig.parameters
        param_names = set(params.keys())
        has_varkw = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values())
    except Exception:
        params = {}
        param_names = set()
        has_varkw = False

    entry_side = "Buy" if side == "long" else "Sell"
    exit_side = "Sell" if side == "long" else "Buy"

    payload: Dict[str, Any] = {
        "account_label": account_label,
        "strategy": strategy,
        "trade_id": trade_id,
        "client_trade_id": client_trade_id,
        "source_trade_id": source_trade_id,
        "symbol": symbol,
        "entry_side": entry_side,
        "entry_qty": float(qty),
        "entry_px": float(entry_px),
        "opened_ts_ms": int(opened_ms),
        "exit_side": exit_side,
        "exit_qty": float(qty),
        "exit_px": float(exit_px),
        "closed_ts_ms": int(closed_ms),
        "fees_usd": float(fees_usd),
        "mode": str(mode),
        "close_reason": str(close_reason),
        "pnl_usd": float(pnl_usd),
        "r_multiple": r_multiple,
        "setup_type": (setup_type or ""),
        "timeframe": (timeframe or ""),
        "ai_profile": ai_profile,
    }

    # Preferred calling:
    # - If writer accepts payload=... use that (most stable).
    # - Else if writer has **kwargs, pass payload as kwargs.
    # - Else filter by known param names (best effort).
    if "payload" in param_names:
        call_kwargs = {"payload": payload}
    elif has_varkw:
        call_kwargs = dict(payload)
    elif param_names:
        call_kwargs = {k: v for k, v in payload.items() if k in param_names}
    else:
        # Conservative fallback (still valid outcome.v1 fields)
        call_kwargs = {
            "trade_id": trade_id,
            "symbol": symbol,
            "entry_side": entry_side,
            "entry_qty": float(qty),
            "entry_px": float(entry_px),
            "opened_ts_ms": int(opened_ms),
            "exit_side": exit_side,
            "exit_qty": float(qty),
            "exit_px": float(exit_px),
            "closed_ts_ms": int(closed_ms),
            "pnl_usd": float(pnl_usd),
            "fees_usd": float(fees_usd),
        }

    try:
        fn(**call_kwargs)  # type: ignore
        log.info("[paper_broker] ✅ outcomes.v1 wrote trade_id=%s", trade_id)
    except Exception as e:
        log.exception(
            "[paper_broker] outcomes.v1 writer failed trade_id=%s account=%s symbol=%s setup_type=%s timeframe=%s",
            trade_id,
            account_label,
            symbol,
            payload.get("setup_type"),
            payload.get("timeframe"),
        )
        _append_jsonl_bytesafe(
            _OUTCOME_WRITE_FAIL_PATH,
            {
                "event_type": "outcome_writer_failed",
                "ts_ms": _now_ms(),
                "trade_id": trade_id,
                "account_label": account_label,
                "symbol": symbol,
                "error": repr(e),
                "payload": payload,
                "call_kwargs": call_kwargs,
            },
        )


# ----------------------------
# PaperBroker core'''

s2 = s[:m.start()] + replacement + s[m.end():]
p.write_text(s2, encoding="utf-8")
print("OK: patched _maybe_write_outcome_v1_from_close")
