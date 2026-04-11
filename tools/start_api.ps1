# tools\start_api.ps1
# Start the Cockpit API (FastAPI via uvicorn) on port 8000.

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$VenvPy = Join-Path $RepoRoot ".venv\Scripts\python.exe"
$VenvDir = Join-Path $RepoRoot ".venv"
$SitePackages = Join-Path $VenvDir "Lib\site-packages"
$Port = 8000
$StdOut = Join-Path $RepoRoot "state\api_stdout.log"
$StdErr = Join-Path $RepoRoot "state\api_stderr.log"

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

Set-Location $RepoRoot
New-Item -ItemType Directory -Force -Path (Join-Path $RepoRoot "state") | Out-Null

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

@(Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue |
  Select-Object -ExpandProperty OwningProcess -Unique) |
  ForEach-Object { Stop-Process -Id $_ -Force -ErrorAction SilentlyContinue }

Remove-Item $StdOut, $StdErr -Force -ErrorAction SilentlyContinue

Start-Process -FilePath $RuntimePy -ArgumentList @(
  "-m","uvicorn",
  "app.api.cockpit_api:app",
  "--host","127.0.0.1",
  "--port", "$Port"
) -WorkingDirectory $RepoRoot -WindowStyle Normal `
  -RedirectStandardOutput $StdOut -RedirectStandardError $StdErr

Start-Sleep -Seconds 2
Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue |
  Select-Object LocalAddress, LocalPort, OwningProcess
