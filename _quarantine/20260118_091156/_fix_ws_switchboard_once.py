from pathlib import Path
import re, datetime

p = Path("app/core/ws_switchboard.py")
s = p.read_text(encoding="utf-8", errors="replace")

stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
bak = p.with_suffix(p.suffix + ".bak_" + stamp)
bak.write_text(s, encoding="utf-8")
print("BACKUP:", bak)

# Remove wrongly injected top-level block
bad = re.compile(
    r'(?ms)^# -{10,}\n'
    r'# Bind per-account bus paths now that ACCOUNT_LABEL is known\n'
    r'# -{10,}\n'
    r'if _is_main\(ACCOUNT_LABEL\):.*?EXECUTIONS_PATH = _env_path\("EXEC_BUS_PATH", str\(EXECUTIONS_PATH\)\)\n'
)

s2, n = bad.subn("", s)
print("Removed bad block:", n)

# Remove stray broken LOG.error fragments if present
s2 = re.sub(r'(?m)^\s{16}"Tried BYBIT_MAIN_WEBSOCKET_KEY/SECRET.*\n', "", s2)
s2 = re.sub(r'(?m)^\s{16}"or BYBIT_<LABEL>_API_KEY/SECRET.*\n', "", s2)

needle = "    rotate_thread.start()\n"
idx = s2.find(needle)
if idx == -1:
    raise SystemExit("FATAL: rotate_thread.start() not found")

insert = '''
    # ---------------------------------------------------------------------------
    # Bind per-account bus paths now that ACCOUNT_LABEL is known
    # ---------------------------------------------------------------------------
    if _is_main(ACCOUNT_LABEL):
        POSITIONS_BUS_PATH = _env_path("POSITIONS_BUS_PATH", "positions_bus.json")
        ORDERBOOK_BUS_PATH = _env_path("ORDERBOOK_BUS_PATH", "orderbook_bus.json")
        TRADES_BUS_PATH    = _env_path("TRADES_BUS_PATH",    "trades_bus.json")
    else:
        POSITIONS_BUS_PATH = _env_path("POSITIONS_BUS_PATH", f"positions_bus_{ACCOUNT_LABEL}.json")
        ORDERBOOK_BUS_PATH = _env_path("ORDERBOOK_BUS_PATH", f"orderbook_bus_{ACCOUNT_LABEL}.json")
        TRADES_BUS_PATH    = _env_path("TRADES_BUS_PATH",    f"trades_bus_{ACCOUNT_LABEL}.json")

    PUBLIC_TRADES_PATH = _env_path("PUBLIC_TRADES_PATH", f"public_trades_{ACCOUNT_LABEL}.jsonl")

    _default_exec = _env_path("EXECUTIONS_PATH", f"ws_executions_{ACCOUNT_LABEL}.jsonl")
    EXECUTIONS_PATH = _env_path("EXECUTIONS_BUS_PATH", str(_default_exec))
    EXECUTIONS_PATH = _env_path("EXEC_BUS_PATH", str(EXECUTIONS_PATH))
'''

s2 = s2[:idx + len(needle)] + insert + s2[idx + len(needle):]

p.write_text(s2, encoding="utf-8")
print("OK: ws_switchboard.py patched successfully")
