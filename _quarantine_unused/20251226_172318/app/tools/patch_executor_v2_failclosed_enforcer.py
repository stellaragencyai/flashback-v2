from pathlib import Path
import shutil
import sys

p = Path(r"app\bots\executor_v2.py")
bak = Path(str(p) + ".bak_before_failclosed_enforcer")
if not bak.exists():
    shutil.copy2(p, bak)

s = p.read_text(encoding="utf-8", errors="ignore")

old = "        except Exception as e:\n            enforced = {\"allow\": True, \"size_multiplier\": 1.0, \"decision_code\": None, \"reason\": f\"enforcer_error:{e}\"}\n"
new = "        except Exception as e:\n            # FAIL POLICY:\n            # - DRY/PAPER: fail-open to keep test loops running\n            # - LIVE: fail-closed so a broken enforcer cannot leak trades\n            enforced = {\"allow\": bool(EXEC_DRY_RUN), \"size_multiplier\": 1.0, \"decision_code\": \"ENFORCER_ERROR\", \"reason\": f\"enforcer_error:{e}\"}\n"

n = s.count(old)
if n != 1:
    print("PATCH_FAIL: expected 1 match, found", n)
    print("Backup:", bak)
    sys.exit(1)

p.write_text(s.replace(old, new), encoding="utf-8")
print("PATCH_OK: executor_v2 fail-closed on enforcer error (LIVE).")
print("Backup:", bak)
