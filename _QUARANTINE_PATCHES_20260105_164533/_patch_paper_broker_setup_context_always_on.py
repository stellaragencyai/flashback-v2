# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
import re

p = Path(r"app\sim\paper_broker.py")
s = p.read_text(encoding="utf-8", errors="ignore")

# ------------------------------------------------------------
# 1) Inject setup_context diagnostics + idempotency helpers
# ------------------------------------------------------------
needle = 'log.warning("[paper_broker] Optional setup_context publish failed: %r", e)'
idx = s.find(needle)
if idx == -1:
    raise SystemExit("FAIL: could not find setup_context warning needle")

# Insert after the except block line
endline = s.find("\n", idx)
endline = s.find("\n", endline + 1)

insert = """

# ----------------------------
# setup_context publish guard + diagnostics
# ----------------------------
_SETUP_CTX_FAIL_PATH = ROOT / "state" / "ai_events" / "setup_context.write_failures.jsonl"
_SETUP_CTX_FAIL_PATH.parent.mkdir(parents=True, exist_ok=True)
_PUBLISHED_SETUP_TRADE_IDS = set()  # in-process idempotency

def _append_jsonl_bytesafe(path: Path, row: dict) -> None:
    try:
        import json
        with path.open("ab") as f:
            f.write(json.dumps(row, ensure_ascii=False).encode("utf-8") + b"\\n")
    except Exception:
        pass
"""

s = s[:endline] + insert + s[endline:]

# ------------------------------------------------------------
# 2) Replace the "if log_setup: _maybe_publish_setup_context(...)" block
#    with always-on + idempotent publish
# ------------------------------------------------------------
pattern = r"\n\s*if log_setup:\n\s*_maybe_publish_setup_context\(\n(?:\s*.*\n)*?\s*\)\n"
m = re.search(pattern, s)
if not m:
    raise SystemExit("FAIL: could not find log_setup gated publish block")

replacement = """
        # Canonical invariant: setup_context must be emitted once per trade open (PAPER/LEARN_DRY)
        if trade_id_final not in _PUBLISHED_SETUP_TRADE_IDS:
            try:
                _maybe_publish_setup_context(
                    trade_id=trade_id_final,
                    symbol=symbol,
                    account_label=self._state.account_label,
                    strategy=self._state.strategy_name,
                    features=features_ext,
                    setup_type=setup_type,
                    timeframe=timeframe,
                    ai_profile=self._state.ai_profile,
                    extra=extra,
                )
                _PUBLISHED_SETUP_TRADE_IDS.add(trade_id_final)
            except Exception as e:
                # Fail-soft but auditable
                _append_jsonl_bytesafe(_SETUP_CTX_FAIL_PATH, {
                    "event_type": "setup_context_write_failed",
                    "trade_id": trade_id_final,
                    "account_label": self._state.account_label,
                    "symbol": symbol,
                    "error": repr(e),
                })
"""

s = re.sub(pattern, "\n" + replacement + "\n", s, count=1)

p.write_text(s, encoding="utf-8")
print("OK: patched paper_broker.py (setup_context always-on + idempotent + diagnostics)")
