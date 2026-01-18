from pathlib import Path

p = Path(r"app\core\ws_switchboard.py")
s = p.read_text(encoding="utf-8")

needle = '    topic = msg.get("topic")\n    if not topic:\n        return\n'

insert = (
    '    topic = msg.get("topic")\n'
    '\n'
    '    # DEBUG (throttled): sample PRIVATE WS messages to confirm Bybit payload shapes/topics\n'
    '    # Env:\n'
    '    #   WS_DEBUG_PRIVATE=true\n'
    '    #   WS_DEBUG_PRIVATE_EVERY=50\n'
    '    global _WS_PRIVATE_SEEN\n'
    '    try:\n'
    '        _WS_PRIVATE_SEEN += 1\n'
    '    except Exception:\n'
    '        _WS_PRIVATE_SEEN = 1\n'
    '\n'
    '    if _env_bool("WS_DEBUG_PRIVATE", "false"):\n'
    '        every = int(os.getenv("WS_DEBUG_PRIVATE_EVERY", "50") or "50")\n'
    '        if every < 1:\n'
    '            every = 1\n'
    '        if (_WS_PRIVATE_SEEN % every) == 0:\n'
    '            try:\n'
    '                LOG.info("[PRIVATE][DBG] topic=%s keys=%s sample=%s", str(topic), list(msg.keys()), str(msg)[:700])\n'
    '            except Exception:\n'
    '                pass\n'
    '\n'
    '    if not topic:\n'
    '        # Also sample no-topic private messages (auth/subscribe acks, etc.)\n'
    '        if _env_bool("WS_DEBUG_PRIVATE", "false"):\n'
    '            every = int(os.getenv("WS_DEBUG_PRIVATE_EVERY", "50") or "50")\n'
    '            if every < 1:\n'
    '                every = 1\n'
    '            if (_WS_PRIVATE_SEEN % every) == 0:\n'
    '                try:\n'
    '                    LOG.info("[PRIVATE][DBG] NO_TOPIC keys=%s sample=%s", list(msg.keys()), str(msg)[:700])\n'
    '                except Exception:\n'
    '                    pass\n'
    '        return\n'
)

if needle not in s:
    raise SystemExit("NEEDLE_NOT_FOUND: private handler topic block changed")

p.write_text(s.replace(needle, insert), encoding="utf-8")
print("PATCHED_OK:", p)
