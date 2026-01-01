# -*- coding: utf-8 -*-
from __future__ import annotations

import re
from pathlib import Path

PB = Path(r"app\sim\paper_broker.py")

def die(msg: str) -> None:
    raise SystemExit(msg)

def main() -> None:
    if not PB.exists():
        die(f"FAIL: not found: {PB}")

    s = PB.read_text(encoding="utf-8", errors="ignore")

    # 1) Inject diagnostics block exactly once
    if "_OUTCOME_WRITE_FAIL_PATH" not in s:
        needle = "# ----------------------------\n# Outcome writer (v1)"
        idx = s.find(needle)
        if idx == -1:
            die("FAIL: could not find outcome writer marker")

        block = (
            "\n# ----------------------------\n"
            "# Outcome writer diagnostics\n"
            "# ----------------------------\n"
            "_OUTCOME_WRITE_FAIL_PATH = ROOT / \"state\" / \"ai_events\" / \"outcomes.v1.write_failures.jsonl\"\n"
            "_OUTCOME_WRITE_FAIL_PATH.parent.mkdir(parents=True, exist_ok=True)\n"
            "\n"
            "def _append_jsonl_bytesafe(path, row) -> None:\n"
            "    try:\n"
            "        import json\n"
            "        with path.open(\"ab\") as f:\n"
            "            f.write(json.dumps(row, ensure_ascii=False).encode(\"utf-8\") + b\"\\n\")\n"
            "    except Exception:\n"
            "        pass\n"
        )

        s = s[:idx] + block + s[idx:]
        print("OK: injected diagnostics block")
    else:
        print("SKIP: diagnostics block already present")

    # 2) Normalize setup_type/timeframe in payload: never None
    # Replace: "setup_type": setup_type,  => "setup_type": (setup_type or ""),
    s2 = re.sub(r'("setup_type"\s*:\s*)setup_type(\s*,)', r'\1(setup_type or "")\2', s, count=1)
    # Replace: "timeframe": timeframe,  => "timeframe": (timeframe or ""),
    s3 = re.sub(r'("timeframe"\s*:\s*)timeframe(\s*,)', r'\1(timeframe or "")\2', s2, count=1)
    if s3 != s:
        print("OK: payload normalization patched")
    else:
        print("WARN: payload normalization pattern not applied (may already be patched or formatted differently)")
    s = s3

    # 3) Replace silent import failure (except: return) with logging + failure jsonl
    pat_import = (
        r"try:\n"
        r"\s*from app\.ai\.outcome_writer import write_outcome_from_paper_close[^\n]*\n"
        r"\s*except Exception:\n"
        r"\s*return"
    )
    repl_import = (
        "try:\n"
        "        from app.ai.outcome_writer import write_outcome_from_paper_close  # type: ignore\n"
        "    except Exception as e:\n"
        "        log.warning(\"[paper_broker] outcomes.v1 writer import failed: %r\", e)\n"
        "        _append_jsonl_bytesafe(_OUTCOME_WRITE_FAIL_PATH, {\"event_type\":\"outcome_writer_import_failed\",\"ts_ms\":_now_ms(),\"error\":repr(e)})\n"
        "        return"
    )
    s_new = re.sub(pat_import, repl_import, s, count=1)
    if s_new != s:
        print("OK: import failure logging patched")
    else:
        print("SKIP: import failure logging already patched (or pattern mismatch)")
    s = s_new

    # 4) Replace warning-only write failure with traceback + forensic record
    pat_fail = r'except Exception as e:\n\s*log\.warning\("\[paper_broker\] outcomes\.v1 writer failed: %r", e\)'
    repl_fail = (
        "except Exception as e:\n"
        "        log.exception(\"[paper_broker] outcomes.v1 writer failed trade_id=%s account=%s symbol=%s setup_type=%s timeframe=%s\", "
        "trade_id, account_label, symbol, payload.get(\"setup_type\"), payload.get(\"timeframe\"))\n"
        "        _append_jsonl_bytesafe(_OUTCOME_WRITE_FAIL_PATH, {"
        "\"event_type\":\"outcome_writer_failed\",\"ts_ms\":_now_ms(),"
        "\"trade_id\":trade_id,\"account_label\":account_label,\"symbol\":symbol,"
        "\"error\":repr(e),\"payload\":payload,\"call_kwargs\":call_kwargs})"
    )
    s_new = re.sub(pat_fail, repl_fail, s, count=1)
    if s_new != s:
        print("OK: failure traceback + forensic jsonl patched")
    else:
        print("SKIP: failure traceback patch already applied (or pattern mismatch)")
    s = s_new

    PB.write_text(s, encoding="utf-8")
    print("OK: wrote app\\sim\\paper_broker.py")

if __name__ == "__main__":
    main()
