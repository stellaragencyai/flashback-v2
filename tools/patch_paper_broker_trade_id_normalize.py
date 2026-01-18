from __future__ import annotations

import re
from pathlib import Path

P = Path("app/sim/paper_broker.py")
s = P.read_text(encoding="utf-8")

if "_normalize_trade_ids_in_state" in s:
    print("SKIP: paper_broker already has trade_id normalization")
    raise SystemExit(0)

# 1) Insert helper methods into PaperBroker (right before _generate_trade_id)
marker = r"(\n\s+def _generate_trade_id\(self, symbol: str\) -> str:\n)"
m = re.search(marker, s)
if not m:
    raise SystemExit("FAIL: could not find _generate_trade_id marker in paper_broker.py")

insert = r"""
    def _normalize_trade_id(self, *, symbol: str, trade_id: str) -> str:
        \"\"\"
        Canonical PAPER trade_id format:
          {account_label}-{symbol}-{suffix}

        Legacy formats (auto-upgraded):
          {account_label}:{suffix}  -> {account_label}-{symbol}-{suffix}

        Notes:
        - Keeps determinism per position by using existing suffix when present.
        - Preserves legacy ID in source_trade_id (see _normalize_trade_ids_in_state).
        \"\"\"
        tid = (trade_id or "").strip()
        if not tid:
            return tid

        acct = str(self._state.account_label)

        # Already canonical-ish (starts with "{acct}-")
        if tid.startswith(acct + "-"):
            return tid

        # Legacy colon format "{acct}:{suffix}"
        legacy_prefix = acct + ":"
        if tid.startswith(legacy_prefix):
            suffix = tid[len(legacy_prefix):]
            suffix = suffix.strip()
            if suffix:
                return f"{acct}-{symbol}-{suffix}"
            return f"{acct}-{symbol}"

        # Unknown format: leave it untouched
        return tid


    def _normalize_trade_ids_in_state(self) -> None:
        \"\"\"
        One-time (but safe to run repeatedly) migration:
        - Upgrades any legacy colon trade_id in open/closed positions to canonical dash format.
        - Preserves original legacy id in source_trade_id if empty.
        - Ensures client_trade_id follows the normalized trade_id when empty.
        \"\"\"
        changed = 0

        positions = []
        try:
            positions.extend(list(self._state.open_positions))
        except Exception:
            pass
        try:
            positions.extend(list(self._state.closed_trades))
        except Exception:
            pass

        for pos in positions:
            try:
                old = str(getattr(pos, "trade_id", "") or "")
                sym = str(getattr(pos, "symbol", "") or "")
                if not old or not sym:
                    continue
                new = self._normalize_trade_id(symbol=sym, trade_id=old)
                if new and new != old:
                    # Preserve legacy id
                    if hasattr(pos, "source_trade_id"):
                        cur_src = getattr(pos, "source_trade_id", None)
                        if not cur_src:
                            setattr(pos, "source_trade_id", old)

                    # Keep client_trade_id coherent if empty
                    if hasattr(pos, "client_trade_id"):
                        cur_cli = getattr(pos, "client_trade_id", None)
                        if not cur_cli:
                            setattr(pos, "client_trade_id", new)

                    setattr(pos, "trade_id", new)
                    changed += 1
            except Exception:
                continue

        if changed:
            try:
                self._save()
            except Exception:
                # If save fails, we still don't want to crash load_or_create.
                pass
"""

s = re.sub(marker, "\n" + insert + r"\1", s, count=1)

# 2) Normalize trade_id during open_position where trade_id_final is computed
# Replace:
#   trade_id_final = trade_id or self._generate_trade_id(symbol)
# with:
#   trade_id_final = trade_id or self._generate_trade_id(symbol)
#   trade_id_final = self._normalize_trade_id(symbol=symbol, trade_id=trade_id_final)
pat = r"(\n\s*trade_id_final\s*=\s*trade_id\s*or\s*self\._generate_trade_id\(symbol\)\s*\n)"
m2 = re.search(pat, s)
if not m2:
    raise SystemExit("FAIL: could not find trade_id_final assignment in open_position")

rep = r"\1" + "        trade_id_final = self._normalize_trade_id(symbol=symbol, trade_id=trade_id_final)\n"
s = re.sub(pat, rep, s, count=1)

# 3) Ensure load_or_create calls migration after broker is created
# Replace "return cls(state, state_path)" with broker init + normalize + return
s2 = s

s2, n = re.subn(
    r"\n(\s*)return\s+cls\(\s*state\s*,\s*state_path\s*\)\s*\n",
    r"\n\1broker = cls(state, state_path)\n\1broker._normalize_trade_ids_in_state()\n\1return broker\n",
    s2,
    count=1
)

# If not found, try alternative formatting (some versions might use keyword args)
if n == 0:
    s2, n2 = re.subn(
        r"\n(\s*)return\s+cls\(\s*state\s*=\s*state\s*,\s*state_path\s*=\s*state_path\s*\)\s*\n",
        r"\n\1broker = cls(state=state, state_path=state_path)\n\1broker._normalize_trade_ids_in_state()\n\1return broker\n",
        s2,
        count=1
    )
    if n2 == 0:
        raise SystemExit("FAIL: could not patch load_or_create return cls(...) to insert normalization call")

P.write_text(s2, encoding="utf-8", newline="\n")
print("OK: patched paper_broker trade_id normalization + migration")
