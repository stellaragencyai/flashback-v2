from pathlib import Path

p = Path("app/core/state_bus.py")
s = p.read_text(encoding="utf-8", errors="ignore")

start = s.find("    def log_ws_execution")
end = s.find("    # ---------- group helpers", start)

print("FOUND_START=", start != -1, "FOUND_END=", end != -1)
assert start != -1 and end != -1 and end > start, "Could not locate method boundaries"

new_block = (
    "    def log_ws_execution(self, label: str, ts_ms: int, row: Dict[str, Any]) -> None:\n"
    "        \"\"\"\n"
    "        Execution log routing:\n"
    "          - main/primary -> state/ws_executions.jsonl (back-compat)\n"
    "          - subs (flashbackXX) -> state/ws_executions_<label>.jsonl\n"
    "\n"
    "        Set ALLOW_GLOBAL_EXEC_BUS=true to force subs onto the global file (not recommended).\n"
    "        \"\"\"\n"
    "        payload = {\"label\": label, \"ts\": ts_ms, \"row\": row}\n"
    "\n"
    "        lab = (label or \"\").strip().lower()\n"
    "        is_main = lab in (\"main\", \"primary\")\n"
    "        allow_global = str(__import__(\"os\").getenv(\"ALLOW_GLOBAL_EXEC_BUS\", \"false\")).strip().lower() in (\"1\",\"true\",\"yes\",\"y\",\"on\")\n"
    "\n"
    "        if is_main or allow_global:\n"
    "            topic = \"ws_executions\"\n"
    "        else:\n"
    "            topic = f\"ws_executions_{lab}\"\n"
    "\n"
    "        self.append_log(topic, payload)\n"
    "\n"
)

s2 = s[:start] + new_block + s[end:]
p.write_text(s2, encoding="utf-8")

print("PATCHED_OK")
