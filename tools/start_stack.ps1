Set-Location C:\flashback

# Start dashboard (your way, not a random new religion)
powershell -NoProfile -ExecutionPolicy Bypass -File .\tools\start_dashboard.ps1

# Start supervisors (the missing piece that stops STALE)
powershell -NoProfile -ExecutionPolicy Bypass -File .\tools\start_supervisors.ps1

# Wait a moment then sanity check one key freshness
Start-Sleep -Seconds 3
python -c "import json,time; from pathlib import Path; d=json.loads(Path(r'C:\flashback\state\ops_snapshot.json').read_text(encoding='utf-8')); c=d.get('components',{}); k='supervisor_ai_stack:flashback01'; row=c.get(k) or {}; ts=row.get('ts_ms',0); age=(time.time()*1000-ts)/1000 if ts else 99999; print('flashback01 supervisor age_sec=',round(age,3)); print('ok=',row.get('ok'));"

