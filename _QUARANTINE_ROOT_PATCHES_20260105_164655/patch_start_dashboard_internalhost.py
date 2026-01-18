from pathlib import Path

p = Path(r"tools/start_dashboard.ps1")
s = p.read_text(encoding="utf-8", errors="ignore")

s2 = s.replace("system.management.automation.internal.host.internalhost", "127.0.0.1")

if s2 == s:
    print("WARN: no internalhost string found (health check may use different hostname)")
else:
    p.write_text(s2, encoding="utf-8")
    print("OK patched start_dashboard.ps1 to use 127.0.0.1")
