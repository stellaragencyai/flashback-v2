from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TARGET = ROOT / "app" / "bots" / "executor_v2.py"

IMPORT = "from app.ai.ai_scoreboard_gatekeeper_v1 import scoreboard_gate_decide\n"
MARKER = "# ✅ NEW canonical policy gate"

def main():
    if not TARGET.exists():
        print(f"ERROR: missing {TARGET}")
        return

    txt = TARGET.read_text(encoding="utf-8", errors="replace")

    if "ai_scoreboard_gatekeeper_v1" not in txt:
        # add import near other app.ai imports, fallback: after first block of imports
        if "from app.ai.ai_executor_gate import" in txt:
            txt = txt.replace("from app.ai.ai_executor_gate import", IMPORT + "from app.ai.ai_executor_gate import")
        else:
            # insert after last import line
            m = re.search(r"(?:^import .+\n|^from .+ import .+\n)+", txt, re.M)
            if m:
                txt = txt[: m.end()] + IMPORT + txt[m.end():]
            else:
                txt = IMPORT + txt

    if "Scoreboard evidence gate (optional)" not in txt:
        idx = txt.find(MARKER)
        if idx == -1:
            print(f"ERROR: marker not found in executor_v2.py: {MARKER}")
            return

        insert = r"""
    # ✅ Scoreboard evidence gate (optional)
    try:
        use_scoreboard_gate = os.getenv("EXEC_SCOREBOARD_GATE", "true").strip().lower() in ("1","true","yes","y")
    except Exception:
        use_scoreboard_gate = True

    scoreboard_gate = None
    if use_scoreboard_gate:
        try:
            scoreboard_gate = scoreboard_gate_decide(
                setup_type=str(setup_type),
                timeframe=str(timeframe),
                symbol=str(symbol),
                account_label=str(account_label) if account_label is not None else None,
            )
        except Exception as e:
            bound_log.warning("Scoreboard gate failed (non-fatal): %r", e)
            scoreboard_gate = None

    if scoreboard_gate is not None:
        sm_sb = scoreboard_gate.get("size_multiplier")
        try:
            if sm_sb is not None:
                size_multiplier_applied = float(size_multiplier_applied) * float(sm_sb)
        except Exception:
            pass

        if not bool(scoreboard_gate.get("allow", True)):
            emit_ai_decision(
                trade_id=client_trade_id,
                account_label=account_label,
                symbol=symbol,
                allow=False,
                decision_code=str(scoreboard_gate.get("decision_code") or "SCOREBOARD_BLOCK"),
                size_multiplier=0.0,
                reason=str(scoreboard_gate.get("reason") or "scoreboard_block"),
                extra={
                    "stage": "scoreboard_gate_pre_policy",
                    "bucket_key": scoreboard_gate.get("bucket_key"),
                    "bucket_stats": scoreboard_gate.get("bucket_stats"),
                    "scoreboard_path": scoreboard_gate.get("scoreboard_path"),
                    "enforced_size_multiplier": float(size_multiplier_applied),
                },
            )
            bound.info("⛔ Scoreboard gate BLOCKED trade_id=%s reason=%s", client_trade_id, scoreboard_gate.get("reason"))
            try:
                tg_send(f"⛔ Scoreboard gate blocked: trade_id={client_trade_id} symbol={symbol} reason={scoreboard_gate.get('reason')}")
            except Exception:
                pass
            return

"""
        txt = txt[:idx] + insert + txt[idx:]

    TARGET.write_text(txt, encoding="utf-8")
    print("OK: patched executor_v2 with scoreboard gate import + hook")

if __name__ == "__main__":
    main()
