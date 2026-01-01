import psutil

def is_running(script_name):
    for p in psutil.process_iter(["cmdline"]):
        try:
            if script_name in " ".join(p.info["cmdline"]):
                return True
        except Exception:
            pass
    return False
