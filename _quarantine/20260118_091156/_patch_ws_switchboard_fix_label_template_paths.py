from pathlib import Path

p = Path(r"app\core\ws_switchboard.py")
s = p.read_text(encoding="utf-8")

needle = '    lab = (account_label or "").strip().lower()\n    POSITIONS_BUS_PATH = _env_path("POSITIONS_BUS_PATH", f"positions_bus_{lab}.json")\n    ORDERBOOK_BUS_PATH = _env_path("ORDERBOOK_BUS_PATH", f"orderbook_bus_{lab}.json")\n    TRADES_BUS_PATH = _env_path("TRADES_BUS_PATH", f"trades_bus_{lab}.json")\n    PUBLIC_TRADES_PATH = _env_path("PUBLIC_TRADES_PATH", f"public_trades_{lab}.jsonl")\n'
insert = (
    '    lab = (account_label or "").strip().lower()\n'
    '    POSITIONS_BUS_PATH = _env_path("POSITIONS_BUS_PATH", f"positions_bus_{lab}.json")\n'
    '    ORDERBOOK_BUS_PATH = _env_path("ORDERBOOK_BUS_PATH", f"orderbook_bus_{lab}.json")\n'
    '    TRADES_BUS_PATH = _env_path("TRADES_BUS_PATH", f"trades_bus_{lab}.json")\n'
    '    PUBLIC_TRADES_PATH = _env_path("PUBLIC_TRADES_PATH", f"public_trades_{lab}.jsonl")\n'
    '\n'
    '    # If env provided a template path like positions_bus_{label}.json, resolve it now.\n'
    '    try:\n'
    '        POSITIONS_BUS_PATH = Path(str(POSITIONS_BUS_PATH).replace("{label}", lab))\n'
    '        ORDERBOOK_BUS_PATH = Path(str(ORDERBOOK_BUS_PATH).replace("{label}", lab))\n'
    '        TRADES_BUS_PATH = Path(str(TRADES_BUS_PATH).replace("{label}", lab))\n'
    '        PUBLIC_TRADES_PATH = Path(str(PUBLIC_TRADES_PATH).replace("{label}", lab))\n'
    '    except Exception:\n'
    '        pass\n'
)

if needle not in s:
    raise SystemExit("NEEDLE_NOT_FOUND: sub bus bind block changed")

p.write_text(s.replace(needle, insert), encoding="utf-8")
print("PATCHED_OK:", p)
