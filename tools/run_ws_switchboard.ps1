# Flashback — Run WS Switchboard for all enabled labels
# Run from project root:
#   powershell -ExecutionPolicy Bypass -File tools/run_ws_switchboard.ps1

$ErrorActionPreference = "Stop"

$ROOT = (Resolve-Path ".").Path
$CFG  = Join-Path $ROOT "config\subaccounts.yaml"

if (!(Test-Path $CFG)) {
  throw "Missing config/subaccounts.yaml at $CFG"
}

# parse YAML via python
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
    Write-Host "[SKIP] $label enabled=false"
    continue
  }

  Write-Host "[START] $label"

  $cmd = @"
cd /d "$ROOT"
set ACCOUNT_LABEL=$label
python .\app\bots\ws_switchboard.py
"@

  Start-Process powershell -ArgumentList "-NoExit", "-Command", $cmd
}
