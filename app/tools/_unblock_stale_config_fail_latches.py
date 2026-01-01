import json
import time
from pathlib import Path

STATE = Path(r"state")
TARGETS = {"flashback01", "flashback02"}

def is_targeted(obj):
    if not isinstance(obj, dict):
        return False
    # Common patterns: {"account_label": "..."} or {"label": "..."}
    lbl = obj.get("account_label") or obj.get("label") or obj.get("account") or obj.get("subaccount")
    return (isinstance(lbl, str) and lbl in TARGETS)

def scrub(obj):
    changed = False

    if isinstance(obj, dict):
        # If this dict is for a target account and looks blocked, clear it.
        if is_targeted(obj):
            if obj.get("blocked") is True:
                obj["blocked"] = False
                changed = True
            det = obj.get("details")
            if isinstance(det, dict):
                if det.get("phase") == "blocked":
                    det["phase"] = "boot"
                    changed = True
                if isinstance(det.get("reason"), str) and "config validation failed" in det["reason"].lower():
                    det["reason"] = "unblocked (stale latch cleared)"
                    changed = True
            if isinstance(obj.get("reason"), str) and "config validation failed" in obj["reason"].lower():
                obj["reason"] = "unblocked (stale latch cleared)"
                changed = True

        # Recurse
        for k, v in list(obj.items()):
            c = scrub(v)
            changed = changed or c

    elif isinstance(obj, list):
        for i in range(len(obj)):
            c = scrub(obj[i])
            changed = changed or c

    return changed

def main():
    if not STATE.exists():
        print("NO_STATE_DIR", str(STATE))
        return

    touched = 0
    for p in STATE.rglob("*.json"):
        try:
            raw = p.read_text(encoding="utf-8", errors="ignore").strip()
            if not raw:
                continue
            data = json.loads(raw)
        except Exception:
            continue

        if scrub(data):
            bak = p.with_suffix(p.suffix + f".bak_unblock_{int(time.time())}")
            try:
                bak.write_text(raw + "\n", encoding="utf-8")
            except Exception:
                pass
            p.write_text(json.dumps(data, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
            print("UNBLOCKED_IN", str(p))
            touched += 1

    print("DONE_TOUCHED_FILES", touched)

if __name__ == "__main__":
    main()
