from pathlib import Path
import re

p = Path(r"C:\Flashback\app\core\ws_switchboard.py")
s = p.read_text(encoding="utf-8", errors="replace")

# Replace ONLY the _ensure_bus_files_exist() function body
pat = re.compile(
    r"def _ensure_bus_files_exist\(\) -> None:\n(?:[ \t].*\n)*",
    re.MULTILINE
)

m = pat.search(s)
if not m:
    raise SystemExit("FATAL: could not find _ensure_bus_files_exist()")

new_func = """def _ensure_bus_files_exist() -> None:
    try:
        # JSON buses
        if not ORDERBOOK_BUS_PATH.exists():
            _atomic_write_json(ORDERBOOK_BUS_PATH, {"version": 1, "updated_ms": 0, "symbols": {}})
        if not TRADES_BUS_PATH.exists():
            _atomic_write_json(TRADES_BUS_PATH, {"version": 1, "updated_ms": 0, "symbols": {}})
        if not POSITIONS_BUS_PATH.exists():
            _atomic_write_json(POSITIONS_BUS_PATH, {"version": 2, "updated_ms": 0, "labels": {}})

        # JSONL buses (touch empty file so downstream checks are deterministic)
        for fp in (PUBLIC_TRADES_PATH, EXECUTIONS_PATH):
            try:
                fp.parent.mkdir(parents=True, exist_ok=True)
                if not fp.exists():
                    fp.write_text("", encoding="utf-8")
            except Exception as e:
                LOG.error("Failed touching jsonl bus %s: %s", str(fp), e)

    except Exception as e:
        LOG.error("Failed ensuring bus files exist: %s", e)


"""

s2 = s[:m.start()] + new_func + s[m.end():]
p.write_text(s2, encoding="utf-8")
print("OK: patched", p)
