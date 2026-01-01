import json, time
from pathlib import Path

FILES = [
  Path(r"state\orchestrator_watchdog.json"),
  Path(r"state\orchestrator_state.json"),
  Path(r"state\fleet_snapshot.json"),
]

TARGETS = {"flashback01","flashback02"}
ts = int(time.time())

def reset_dict(d: dict) -> bool:
  changed = False

  # common latch fields
  for k in ["blocked","is_blocked"]:
    if d.get(k) is True:
      d[k] = False; changed = True

  for k in ["reason","block_reason","blocked_reason","last_error","error"]:
    if isinstance(d.get(k), str) and d[k]:
      # only clear if it looks like a latch reason
      if ("config" in d[k].lower()) or ("blocked" in d[k].lower()) or ("validation" in d[k].lower()):
        d[k] = ""; changed = True

  for k in ["backoff_sec","backoff_seconds","cooldown_sec","cooldown_seconds","restart_backoff_sec"]:
    if isinstance(d.get(k), (int,float)) and d[k] != 0:
      d[k] = 0; changed = True

  for k in ["restart_count","restarts","consecutive_failures","fail_count"]:
    if isinstance(d.get(k), int) and d[k] != 0:
      d[k] = 0; changed = True

  for k in ["next_retry_ts","next_retry_ts_ms","next_restart_ts","next_restart_ts_ms"]:
    if k in d and d.get(k) not in (None, 0, "0"):
      d[k] = 0; changed = True

  return changed

def scrub(obj) -> bool:
  changed = False

  if isinstance(obj, dict):
    # 1) if dict has keys that are targets (keyed-by-label state), reset or delete those entries
    for t in list(TARGETS):
      if t in obj:
        v = obj[t]
        if isinstance(v, dict):
          if reset_dict(v):
            changed = True
          obj[t] = v
        else:
          # reason string or something: clear it
          obj[t] = "" if isinstance(v, str) else v
          changed = True

    # 2) remove targets from any list-like fields
    for k in ["blocked_labels","blocked_accounts","blocked_account_labels","blocked","blocked_list"]:
      v = obj.get(k)
      if isinstance(v, list):
        before = list(v)
        obj[k] = [x for x in v if not (isinstance(x,str) and x in TARGETS)]
        if obj[k] != before:
          changed = True

    # 3) clear nested dicts that map label->reason in common places
    for k in ["blocked_reasons","block_reasons","blocked_map","block_map","reasons","failures"]:
      v = obj.get(k)
      if isinstance(v, dict):
        for t in list(TARGETS):
          if t in v:
            v.pop(t, None)
            changed = True
        obj[k] = v

    # 4) recurse
    for k,v in list(obj.items()):
      if scrub(v):
        changed = True

    # 5) also reset this dict itself if it looks like a latch container
    if reset_dict(obj):
      changed = True

  elif isinstance(obj, list):
    for i in range(len(obj)):
      if scrub(obj[i]):
        changed = True

  return changed

def main():
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
      bak = p.with_suffix(p.suffix + f".bak_hardclear_{ts}")
      bak.write_text(raw + "\n", encoding="utf-8")
      p.write_text(json.dumps(data, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
      print("HARD_CLEARED", str(p))
      touched += 1
    else:
      print("NO_CHANGE", str(p))

  print("DONE_TOUCHED_FILES", touched)

if __name__ == "__main__":
  main()
