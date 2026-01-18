import re
from pathlib import Path

p = Path(r"app\core\ws_switchboard.py")
s = p.read_text(encoding="utf-8")

# Heuristic: locate the PUBLIC connect/runner function by looking for "[PUBLIC]" log strings.
pub_anchor = re.search(r"def\s+(?P<fn>\w+)\([^)]*\):\n(?P<body>(?:    .*\n)+?)\n(?=def\s|\Z)", s)
if not pub_anchor:
    raise SystemExit("CANNOT_FIND: any top-level def blocks (unexpected file structure)")

# Find all def blocks and pick the ones that contain PUBLIC logging strings (best signal).
defs = list(re.finditer(r"^def\s+(?P<name>\w+)\([^)]*\):\n(?P<body>(?:    .*\n)+?)(?=^\S|\Z)", s, flags=re.M))
candidates = []
for m in defs:
    body = m.group("body")
    if "[PUBLIC]" in body or "PUBLIC WS" in body or "public_url" in body or "v5/public" in body:
        candidates.append(m)

if not candidates:
    raise SystemExit("CANNOT_FIND: any def blocks containing PUBLIC markers")

# Use the most likely PUBLIC runner (the one with most PUBLIC markers)
def score(m):
    b = m.group("body")
    return b.count("[PUBLIC]") + b.count("PUBLIC") + b.count("public_url") + b.count("v5/public")
target = sorted(candidates, key=score, reverse=True)[0]

body = target.group("body")

# Now find an inner callback def that takes (ws, message/msg) OR (message) and inject debug at its top.
inner_defs = list(re.finditer(r"^    def\s+(?P<iname>\w+)\((?P<args>[^)]*)\):\n(?P<ibody>(?:        .*\n)+)", body, flags=re.M))
if not inner_defs:
    raise SystemExit("CANNOT_FIND: inner def callbacks inside PUBLIC runner")

# Choose a likely on_message handler
best = None
best_score = -1
for m in inner_defs:
    args = m.group("args")
    ibody = m.group("ibody")
    sc = 0
    if "message" in args or "msg" in args:
        sc += 5
    if "json" in ibody or "loads" in ibody:
        sc += 3
    if "topic" in ibody:
        sc += 3
    if "msg.get" in ibody or "message.get" in ibody:
        sc += 3
    best = m if sc > best_score else best
    best_score = max(best_score, sc)

if not best:
    raise SystemExit("CANNOT_FIND: suitable PUBLIC message callback")

ibody = best.group("ibody")

# Avoid double patch
if "WS_DEBUG_PUBLIC" in ibody and "[PUBLIC][DBG]" in ibody:
    print("ALREADY_PATCHED: WS_DEBUG_PUBLIC")
    raise SystemExit(0)

inject = (
    "        # DEBUG (throttled): sample PUBLIC WS messages to confirm topics/payloads\n"
    "        # Env:\n"
    "        #   WS_DEBUG_PUBLIC=true\n"
    "        #   WS_DEBUG_PUBLIC_EVERY=50\n"
    "        global _WS_PUBLIC_SEEN\n"
    "        try:\n"
    "            _WS_PUBLIC_SEEN += 1\n"
    "        except Exception:\n"
    "            _WS_PUBLIC_SEEN = 1\n"
    "\n"
    "        if _env_bool(\"WS_DEBUG_PUBLIC\", \"false\"):\n"
    "            every = int(os.getenv(\"WS_DEBUG_PUBLIC_EVERY\", \"50\") or \"50\")\n"
    "            if every < 1:\n"
    "                every = 1\n"
    "            if (_WS_PUBLIC_SEEN % every) == 0:\n"
    "                try:\n"
    "                    LOG.info(\"[PUBLIC][DBG] keys=%s sample=%s\", list(locals().get('msg', locals().get('message', {})).keys()) if isinstance(locals().get('msg', locals().get('message', {})), dict) else type(locals().get('msg', locals().get('message', None))), str(locals().get('msg', locals().get('message', '')))[:700])\n"
    "                except Exception:\n"
    "                    pass\n"
    "\n"
)

new_ibody = inject + ibody

# Replace only the inner body match
new_body = body[:best.start("ibody")] + new_ibody + body[best.end("ibody"):]
s2 = s[:target.start("body")] + new_body + s[target.end("body"):]

p.write_text(s2, encoding="utf-8")
print("PATCHED_OK:", p, "PUBLIC_RUNNER=", target.group("name"), "INNER_CB=", best.group("iname"))
