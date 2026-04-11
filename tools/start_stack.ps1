Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$VenvPy = Join-Path $RepoRoot ".venv\Scripts\python.exe"
$VenvDir = Join-Path $RepoRoot ".venv"
$SitePackages = Join-Path $VenvDir "Lib\site-packages"

if (-not (Test-Path $VenvPy)) {
  throw "Missing repo venv python at $VenvPy"
}

if (-not (Test-Path $SitePackages)) {
  throw "Missing venv site-packages at $SitePackages"
}

$RuntimePy = (& $VenvPy -c "import sys; print(getattr(sys, '_base_executable', sys.executable))").Trim()
if (-not $RuntimePy) {
  throw "Could not resolve runtime python from $VenvPy"
}
if (-not (Test-Path $RuntimePy)) {
  throw "Resolved runtime python does not exist: $RuntimePy"
}

$env:FLASHBACK_DIRECT_BASE_PYTHON = "1"
$env:FLASHBACK_RUNTIME_BASE_PYTHON = $RuntimePy
$env:FLASHBACK_RUNTIME_VENV = $VenvDir
$env:FLASHBACK_RUNTIME_SITE_PACKAGES = $SitePackages
$env:VIRTUAL_ENV = $VenvDir
$env:PYTHONNOUSERSITE = "1"
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"
$env:PYTHONPATH = if ([string]::IsNullOrWhiteSpace($env:PYTHONPATH)) {
  "$RepoRoot;$SitePackages"
} else {
  "$RepoRoot;$SitePackages;$env:PYTHONPATH"
}
$env:PATH = if ([string]::IsNullOrWhiteSpace($env:PATH)) {
  (Join-Path $VenvDir "Scripts")
} else {
  "$(Join-Path $VenvDir 'Scripts');$env:PATH"
}

Set-Location $RepoRoot

& powershell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $RepoRoot "tools\start_dashboard.ps1")
& powershell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $RepoRoot "tools\start_supervisors.ps1")

Start-Sleep -Seconds 4

& $RuntimePy -c @"
import json
import time
from pathlib import Path

path = Path(r"$RepoRoot") / "state" / "ops_snapshot.json"
if not path.exists():
    raise SystemExit("ops_snapshot.json not found after startup")

d = json.loads(path.read_text(encoding="utf-8"))
c = d.get("components", {})
k = "supervisor_ai_stack:flashback01"
row = c.get(k) or {}
ts = row.get("ts_ms", 0)
age = ((time.time() * 1000) - ts) / 1000 if ts else 99999
print("flashback01 supervisor age_sec=", round(age, 3))
print("ok=", row.get("ok"))
"@
