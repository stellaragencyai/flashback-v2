from pathlib import Path

p = Path(r"app/dashboard/templates/dashboard.html")
s = p.read_text(encoding="utf-8", errors="ignore")
orig = s

# Fail-soft dict access fixes (stop Jinja UndefinedError)
s = s.replace("{% set sup_ok = (true if r.supervisor_ok else false) %}",
              "{% set sup_ok = (true if (r.get('supervisor_ok')) else false) %}")

s = s.replace("{% set sup_age = (r.supervisor_age_sec if r.supervisor_age_sec is not none else none) %}",
              "{% set sup_age = (r.get('supervisor_age_sec')) %}")

s = s.replace('{{ "ON" if (r.enable_ai_stack) else "OFF" }}',
              '{{ "ON" if (r.get("enable_ai_stack")) else "OFF" }}')

s = s.replace("{{ r.workers_running if r.workers_running is not none else 0 }}",
              "{{ r.get('workers_running') if r.get('workers_running') is not none else 0 }}")

s = s.replace("{{ r.schema_version if r.schema_version is not none else \"—\" }}",
              "{{ r.get('schema_version') if r.get('schema_version') is not none else \"—\" }}")

# If the template has any other direct access to supervisor_age_sec, make it safe
s = s.replace("r.supervisor_age_sec", "r.get('supervisor_age_sec')")

if s == orig:
    print("WARN: No changes applied (patterns not found). We need to patch manually by searching.")
else:
    p.write_text(s, encoding="utf-8")
    print("OK patched dashboard.html to fail-soft for missing keys")
