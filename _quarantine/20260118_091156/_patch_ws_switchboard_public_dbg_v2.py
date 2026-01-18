from pathlib import Path

p = Path(r"app\core\ws_switchboard.py")
s = p.read_text(encoding="utf-8")

needle = 'def _handle_public_message(msg: Dict[str, Any]) -> None:\n    global _WS_ORDERBOOK_SEEN\n\n    topic = msg.get("topic")\n    if not topic:\n        return\n'
insert = (
'def _handle_public_message(msg: Dict[str, Any]) -> None:\n'
'    global _WS_ORDERBOOK_SEEN\n\n'
'    topic = msg.get("topic")\n'
'\n'
'    # DEBUG (throttled): sample PUBLIC WS messages to confirm topics/payload shapes\n'
'    # Env:\n'
'    #   WS_DEBUG_PUBLIC=true\n'
'    #   WS_DEBUG_PUBLIC_EVERY=50\n'
'    global _WS_PUBLIC_SEEN\n'
'    try:\n'
'        _WS_PUBLIC_SEEN += 1\n'
'    except Exception:\n'
'        _WS_PUBLIC_SEEN = 1\n'
'\n'
'    if _env_bool("WS_DEBUG_PUBLIC", "false"):\n'
'        every = int(os.getenv("WS_DEBUG_PUBLIC_EVERY", "50") or "50")\n'
'        if every < 1:\n'
'            every = 1\n'
'        if (_WS_PUBLIC_SEEN % every) == 0:\n'
'            try:\n'
'                LOG.info("[PUBLIC][DBG] topic=%s keys=%s sample=%s", str(topic), list(msg.keys()), str(msg)[:700])\n'
'            except Exception:\n'
'                pass\n'
'\n'
'    if not topic:\n'
'        return\n'
)

if needle not in s:
    raise SystemExit("NEEDLE_NOT_FOUND: public handler header changed")

p.write_text(s.replace(needle, insert), encoding="utf-8")
print("PATCHED_OK:", p)
