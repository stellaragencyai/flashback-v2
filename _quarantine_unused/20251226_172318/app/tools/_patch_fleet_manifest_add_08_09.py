from pathlib import Path
import yaml

p = Path(r"config\fleet_manifest.yaml")
d = yaml.safe_load(p.read_text(encoding="utf-8", errors="ignore")) or {}
fleet = d.get("fleet") or []
if not isinstance(fleet, list):
    raise SystemExit("FATAL: manifest fleet is not a list")

existing = set()
for r in fleet:
    if isinstance(r, dict):
        lbl = str(r.get("account_label") or "").strip()
        if lbl:
            existing.add(lbl)

to_add = []
for lbl in ["flashback08", "flashback09"]:
    if lbl not in existing:
        # Defaults: show up in fleet but DO NOT RUN yet
        to_add.append({
            "account_label": lbl,
            "enabled": False,
            "enable_ai_stack": True,
            "automation_mode": "LEARN_DRY",
        })

if not to_add:
    print("OK: flashback08/09 already present in fleet_manifest.yaml")
    raise SystemExit(0)

fleet.extend(to_add)
d["fleet"] = fleet

p.write_text(yaml.safe_dump(d, sort_keys=False), encoding="utf-8")
print("OK: added to fleet_manifest.yaml ->", ", ".join([x["account_label"] for x in to_add]))
