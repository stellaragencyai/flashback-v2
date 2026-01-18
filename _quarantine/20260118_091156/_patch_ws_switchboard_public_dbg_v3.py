from pathlib import Path

p = Path(r"app\core\ws_switchboard.py")
s = p.read_text(encoding="utf-8")

fn = "def _handle_public_message"
start = s.find(fn)
if start < 0:
    raise SystemExit("CANNOT_FIND: def _handle_public_message")

topic_line = '    topic = msg.get("topic")'
pos_topic = s.find(topic_line, start)
if pos_topic < 0:
    raise SystemExit("CANNOT_FIND: public topic assignment line")

# Insert right AFTER the topic assignment line
line_end = s.find("\n", pos_topic)
if line_end < 0:
    raise SystemExit("CANNOT_FIND: newline after topic assignment")

insert = (
    "\n"
    "    # DEBUG (throttled): sample PUBLIC WS messages to confirm topics/payload shapes\n"
    "    # Env:\n"
    "    #   WS_DEBUG_PUBLIC=true\n"
    "    #   WS_DEBUG_PUBLIC_EVERY=50\n"
    "    global _WS_PUBLIC_SEEN\n"
    "    try:\n"
    "        _WS_PUBLIC_SEEN += 1\n"
    "    except Exception:\n"
    "        _WS_PUBLIC_SEEN = 1\n"
    "\n"
    "    if _env_bool(\"WS_DEBUG_PUBLIC\", \"false\"):\n"
    "        every = int(os.getenv(\"WS_DEBUG_PUBLIC_EVERY\", \"50\") or \"50\")\n"
    "        if every < 1:\n"
    "            every = 1\n"
    "        if (_WS_PUBLIC_SEEN % every) == 0:\n"
    "            try:\n"
    "                LOG.info(\"[PUBLIC][DBG] topic=%s keys=%s sample=%s\", str(topic), list(msg.keys()), str(msg)[:700])\n"
    "            except Exception:\n"
    "                pass\n"
)

s2 = s[: line_end + 1] + insert + s[line_end + 1 :]
p.write_text(s2, encoding="utf-8")
print("PATCHED_OK:", p)
