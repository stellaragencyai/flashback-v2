import json, time
from pathlib import Path

FILES = [
  Path(r"state\orchestrator_watchdog.json"),
  Path(r"state\orchestrator_state.json"),
  Path(r"state\fleet_snapshot.json"),
]

TARGETS = {"flashback01","flashback02"}

def is_targeted(d: dict) -> bool:
  lbl = d.get("account_label") or d.get("label") or d.get("account") or d.get("subaccount")
  return isinstance(lbl,str) and lbl in TARGETS

def scrub(obj):
  changed = False

  if isinstance(obj, dict):
    # If this is a targeted node, clear common latches
    if is_targeted(obj):
      if obj.get("blocked") is True:
        obj["blocked"] = False; changed = True

      for k in ["block_reason","reason","last_error","error","blocked_reason","blocked_by"]:
        if isinstance(obj.get(k), str) and "config validation" in obj.get(k).lower():
          obj[k] = ""; changed = True

      # reset timers/counters if present
      for k in ["backoff_sec","backoff_seconds","restart_backoff_sec","cooldown_sec","cooldown_seconds"]:
        if k in obj and isinstance(obj.get(k), (int,float)) and obj[k] != 0:
          obj[k] = 0; changed = True

      for k in ["next_retry_ts","next_retry_ts_ms","next_restart_ts","next_restart_ts_ms"]:
        if k in obj and obj.get(k) not in (None, 0, "0"):
          obj[k] = 0; changed = True

      for k in ["restart_count","restarts","consecutive_failures","fail_count"]:
        if k in obj and isinstance(obj.get(k), int) and obj[k] != 0:
          obj[k] = 0; changed = True

      # details sub-dict
      det = obj.get("details")
      if isinstance(det, dict):
        if det.get("phase") == "blocked":
          det["phase"] = "boot"; changed = True
        if isinstance(det.get("reason"), str) and "config validation" in det["reason"].lower():
          det["reason"] = "cleared"; changed = True

    # global lists that may include blocked labels
    for k in ["blocked_labels","blocked","blocked_accounts","blocked_account_labels"]:
      v = obj.get(k)
      if isinstance(v, list):
        before = list(v)
        obj[k] = [x for x in v if not (isinstance(x,str) and x in TARGETS)]
        if obj[k] != before:
          changed = True

    # recurse
    for k,v in list(obj.items()):
      if scrub(v):
        changed = True

  elif isinstance(obj, list):
    for i in range(len(obj)):
      if scrub(obj[i]):
        changed = True

  return changed

def main():
  ts = int(time.time())
  touched = 0
  for p in FILES:
    if not p.exists():
      print("SKIP_MISSING", str(p)); continue
    raw = p.read_text(encoding="utf-8", errors="ignore").strip()
    if not raw:
      print("SKIP_EMPTY", str(p)); continue
    try:
      data = json.loads(raw)
    except Exception as e:
      print("SKIP_BADJSON", str(p), e); continue

    if scrub(data):
      bak = p.with_suffix(p.suffix + f".bak_unblock_{ts}")
      bak.write_text(raw + "\n", encoding="utf-8")
      p.write_text(json.dumps(data, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
      print("UNBLOCKED_IN", str(p))
      touched += 1
    else:
      print("NO_CHANGE", str(p))

  print("DONE_TOUCHED_FILES", touched)

if __name__ == "__main__":
  main()
