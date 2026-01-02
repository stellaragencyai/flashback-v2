import time, subprocess, sys
while True:
    try:
        subprocess.run([sys.executable, r".\app\tools\bootstrap_ops_snapshot_from_outcomes_v1.py"], check=False)
    except Exception:
        pass
    time.sleep(10)
