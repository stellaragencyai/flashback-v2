from pathlib import Path
import re

p = Path(r"app\core\flashback_common.py")
s = p.read_text(encoding="utf-8", errors="ignore")

# Windows-safe patterns: handle \n or \r\n, and use DOTALL + MULTILINE
pat1 = r"(?ms)^def last_price\(symbol: str\) -> Decimal:\r?\n.*?(?=^\s*def\s|\Z)"
rep1 = (
    "def last_price(symbol: str) -> Decimal:\n"
    "    \"\"\"Best-effort public last price.\n"
    "    DRY_RUN must not depend on Bybit REST.\n"
    "    \"\"\"\n"
    "    if EXEC_DRY_RUN:\n"
    "        return Decimal(\"0\")\n"
    "    try:\n"
    "        r = bybit_get(\n"
    "            \"/v5/market/tickers\",\n"
    "            {\"category\": \"linear\", \"symbol\": symbol},\n"
    "            auth=False,\n"
    "        )\n"
    "        lst = (r.get(\"result\", {}) or {}).get(\"list\", []) or []\n"
    "        if not lst:\n"
    "            return Decimal(\"0\")\n"
    "        return Decimal(str(lst[0].get(\"lastPrice\", \"0\")))\n"
    "    except Exception:\n"
    "        return Decimal(\"0\")\n\n"
)

s, n1 = re.subn(pat1, rep1, s, count=1)

pat2 = r"(?ms)^def last_price_ws_first\(symbol: str\) -> Decimal:\r?\n.*?(?=^\s*def\s|\Z)"
rep2 = (
    "def last_price_ws_first(symbol: str) -> Decimal:\n"
    "    \"\"\"WS-first last price.\n"
    "    In EXEC_DRY_RUN, never fall back to REST.\n"
    "    \"\"\"\n"
    "    ws_mid = mid_price_ws_first(symbol)\n"
    "    if ws_mid is not None and ws_mid > 0:\n"
    "        return ws_mid\n"
    "    if EXEC_DRY_RUN:\n"
    "        return Decimal(\"0\")\n"
    "    return last_price(symbol)\n\n"
)

s, n2 = re.subn(pat2, rep2, s, count=1)

if n1 != 1 or n2 != 1:
    raise SystemExit(f"PATCH_FAIL flashback_common.py: last_price={n1} last_price_ws_first={n2}")

p.write_text(s, encoding="utf-8")
print("PATCH_OK flashback_common.py")
