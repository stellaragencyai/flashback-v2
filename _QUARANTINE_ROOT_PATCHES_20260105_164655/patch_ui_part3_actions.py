from pathlib import Path
import re

p = Path(r"app/dashboard/templates/dashboard.html")
s = p.read_text(encoding="utf-8", errors="ignore")
orig = s

# Insert action row just before </details> close
marker = "</details>"
if marker not in s:
    print("PATCH_FAIL: details block not found")
    raise SystemExit(1)

action_html = r'''
        <div class="actionRow" data-acct="{{ acct }}">
          <button class="actBtn start" data-action="start">Start</button>
          <button class="actBtn stop" data-action="stop">Stop</button>
          <button class="actBtn restart" data-action="restart">Restart</button>
          <button class="actBtn ping" data-action="ping">Ping TG</button>
        </div>
'''

s = s.replace(marker, action_html + "\n" + marker, 1)

# Inject CSS if not present
if "/* ACTION ROW v1 */" not in s:
    s = s.replace(
        "/* PREMIUM UI OVERRIDES v1 */",
        "/* PREMIUM UI OVERRIDES v1 */\n\n/* ACTION ROW v1 */\n"
        ".actionRow{\n"
        "  margin-top: 10px;\n"
        "  display:flex;\n"
        "  gap: 8px;\n"
        "  padding-left: 6px;\n"
        "}\n"
        ".actBtn{\n"
        "  border: 1px solid rgba(148,163,184,.25);\n"
        "  background: rgba(148,163,184,.08);\n"
        "  border-radius: 10px;\n"
        "  padding: 6px 10px;\n"
        "  font-size: 11px;\n"
        "  font-weight: 850;\n"
        "  cursor: pointer;\n"
        "}\n"
        ".actBtn:hover{ border-color: rgba(37,99,235,.35); }\n"
        ".actBtn.start{ color: var(--green); }\n"
        ".actBtn.stop{ color: var(--red); }\n"
        ".actBtn.restart{ color: var(--yellow); }\n"
        ".actBtn.ping{ color: var(--blue); }\n"
    )

if s == orig:
    print("PATCH_NOOP: no changes applied")
else:
    p.write_text(s, encoding="utf-8")
    print("OK: Action row UI added")
