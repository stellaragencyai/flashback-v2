from pathlib import Path

p = Path(r"app\core\ws_switchboard.py")
s = p.read_text(encoding="utf-8")

needle = "def main() -> None:\n    account_label = os.getenv(\"ACCOUNT_LABEL\", \"main\").strip() or \"main\"\n"

insert = (
    "def main() -> None:\n"
    "    import argparse\n\n"
    "    parser = argparse.ArgumentParser(prog=\"ws_switchboard\", description=\"Flashback WS Switchboard\")\n"
    "    parser.add_argument(\"--account-label\", dest=\"account_label\", default=None, help=\"Account label (example: flashback02 or main)\")\n"
    "    args, unknown = parser.parse_known_args()\n\n"
    "    if args.account_label:\n"
    "        os.environ[\"ACCOUNT_LABEL\"] = str(args.account_label).strip()\n\n"
    "    if unknown:\n"
    "        try:\n"
    "            LOG.info(\"Ignoring unknown CLI args: %s\", unknown)\n"
    "        except Exception:\n"
    "            pass\n\n"
    "    account_label = os.getenv(\"ACCOUNT_LABEL\", \"main\").strip() or \"main\"\n"
)

if needle not in s:
    raise SystemExit("NEEDLE_NOT_FOUND: main() header changed")

p.write_text(s.replace(needle, insert), encoding="utf-8")
print("PATCHED_OK:", p)
