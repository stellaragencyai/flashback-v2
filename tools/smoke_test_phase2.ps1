# Flashback — Phase 2 Smoke Test
# Run from project root: powershell -ExecutionPolicy Bypass -File tools/smoke_test_phase2.ps1

$ErrorActionPreference = "Stop"

$ROOT = (Resolve-Path ".").Path
$LOG  = Join-Path $ROOT "state\smoke_test_phase2.log"
$CFG  = Join-Path $ROOT "config\subaccounts.yaml"

New-Item -ItemType Directory -Force -Path (Join-Path $ROOT "state") | Out-Null
"=== Smoke Test Phase 2 ===" | Out-File -FilePath $LOG -Encoding utf8
("ROOT: {0}" -f $ROOT) | Out-File -FilePath $LOG -Append -Encoding utf8
("TIME: {0}" -f (Get-Date)) | Out-File -FilePath $LOG -Append -Encoding utf8
"" | Out-File -FilePath $LOG -Append -Encoding utf8

if (!(Test-Path $CFG)) {
  throw "Missing config/subaccounts.yaml at $CFG"
}

# Parse YAML by asking Python (because PS YAML parsing is a circus)
$labelsJson = python -c "import yaml, json; d=yaml.safe_load(open(r'$CFG','r',encoding='utf-8')) or {}; out={}; 
for k,v in (d or {}).items():
  if k in ('version','notes','legacy'): 
    continue
  if isinstance(v, dict):
    out[k]=v
print(json.dumps(out))"

$labels = $labelsJson | ConvertFrom-Json

foreach ($prop in $labels.PSObject.Properties) {
  $label = $prop.Name
  $meta  = $prop.Value

  $enabled = $true
  if ($null -ne $meta.enabled) { $enabled = [bool]$meta.enabled }

  if (-not $enabled) {
    ("[SKIP] {0}: enabled=false" -f $label) | Tee-Object -FilePath $LOG -Append
    continue
  }

  ("[RUN ] {0}" -f $label) | Tee-Object -FilePath $LOG -Append

  $env:ACCOUNT_LABEL = $label

  # Start ws_switchboard in background job
  $job = Start-Job -Name ("ws_" + $label) -ScriptBlock {
    param($root)
    Set-Location $root
    python .\app\bots\ws_switchboard.py
  } -ArgumentList $ROOT

  Start-Sleep -Seconds 10

  # Run health check for this label
  $hc = & python .\tools\health_check.py --label $label
  $exit = $LASTEXITCODE

  $hc | Out-File -FilePath $LOG -Append -Encoding utf8

  if ($exit -eq 0) {
    ("[PASS] {0}" -f $label) | Tee-Object -FilePath $LOG -Append
  } else {
    ("[FAIL] {0}" -f $label) | Tee-Object -FilePath $LOG -Append
  }

  # Stop ws job
  try { Stop-Job $job -Force | Out-Null } catch {}
  try { Remove-Job $job -Force | Out-Null } catch {}

  "" | Out-File -FilePath $LOG -Append -Encoding utf8
}

"=== DONE ===" | Out-File -FilePath $LOG -Append -Encoding utf8
Write-Host "Smoke test complete. Log: $LOG"
