# tools\start_dashboard.ps1
# Canonical launcher for Flashback Dashboard (Flask-SocketIO) on port 5000.

Set-StrictMode -Version Latest
$ErrorActionPreference = "SilentlyContinue"

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$VenvPy = Join-Path $RepoRoot ".venv\Scripts\python.exe"
$VenvDir = Join-Path $RepoRoot ".venv"
$SitePackages = Join-Path $VenvDir "Lib\site-packages"

Set-Location $RepoRoot

# --- Config ---
$Port   = 5000
$DashboardHost = "127.0.0.1"
$Server = "waitress"
$Threads = 8
$DashPy = Join-Path $RepoRoot "app\dashboard\dashboard_server.py"

$LogDir = Join-Path $RepoRoot "state"
$StdOut = Join-Path $LogDir "dashboard_stdout.log"
$StdErr = Join-Path $LogDir "dashboard_stderr.log"

# --- Ensure state dir exists ---
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

# --- Kill any listeners on port ---
$ListenerPids = @(
  Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue |
    Select-Object -ExpandProperty OwningProcess -Unique
)

if ($ListenerPids.Count -gt 0) {
  Write-Host ("Killing listeners on port {0}: {1}" -f $Port, ($ListenerPids -join ", "))
  foreach ($ProcId in $ListenerPids) {
    Stop-Process -Id $ProcId -Force -ErrorAction SilentlyContinue
  }
  Start-Sleep -Seconds 1
}

# --- Clean logs ---
Remove-Item -Force -ErrorAction SilentlyContinue $StdOut, $StdErr

# --- Validate dashboard file exists ---
if (-not (Test-Path $DashPy)) {
  Write-Host ("ERROR: Dashboard entry not found: {0}" -f $DashPy)
  exit 2
}

if (-not (Test-Path $VenvPy)) {
  Write-Host ("ERROR: Missing repo venv python: {0}" -f $VenvPy)
  exit 2
}

if (-not (Test-Path $SitePackages)) {
  Write-Host ("ERROR: Missing venv site-packages: {0}" -f $SitePackages)
  exit 2
}

$RuntimePy = (& $VenvPy -c "import sys; print(getattr(sys, '_base_executable', sys.executable))").Trim()
if (-not $RuntimePy -or -not (Test-Path $RuntimePy)) {
  Write-Host ("ERROR: Could not resolve runtime python from: {0}" -f $VenvPy)
  exit 2
}

# --- Start dashboard (new window) ---
Write-Host ("Starting dashboard: {0}" -f $DashPy)

$ChildEnv = @{
  "FLASHBACK_DIRECT_BASE_PYTHON" = "1"
  "FLASHBACK_RUNTIME_BASE_PYTHON" = $RuntimePy
  "FLASHBACK_RUNTIME_VENV" = $VenvDir
  "FLASHBACK_RUNTIME_SITE_PACKAGES" = $SitePackages
  "FLASHBACK_DASHBOARD_HOST" = $DashboardHost
  "FLASHBACK_DASHBOARD_PORT" = [string]$Port
  "FLASHBACK_DASHBOARD_SERVER" = $Server
  "FLASHBACK_DASHBOARD_THREADS" = [string]$Threads
  "VIRTUAL_ENV" = $VenvDir
  "PYTHONNOUSERSITE" = "1"
  "PYTHONUTF8" = "1"
  "PYTHONIOENCODING" = "utf-8"
  "PYTHONPATH" = if ([string]::IsNullOrWhiteSpace($env:PYTHONPATH)) { "$RepoRoot;$SitePackages" } else { "$RepoRoot;$SitePackages;$env:PYTHONPATH" }
  "PATH" = if ([string]::IsNullOrWhiteSpace($env:PATH)) { (Join-Path $VenvDir "Scripts") } else { "$(Join-Path $VenvDir 'Scripts');$env:PATH" }
}
$ChildEnv.GetEnumerator() | ForEach-Object {
  Set-Item -Path ("Env:{0}" -f $_.Key) -Value ([string]$_.Value)
}

Start-Process -FilePath $RuntimePy -ArgumentList @("-u", $DashPy) `
  -WorkingDirectory $RepoRoot `
  -WindowStyle Normal | Out-Null

$Deadline = (Get-Date).AddSeconds(20)
$Conn = $null
do {
  Start-Sleep -Milliseconds 500
  $Conn = Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue | Select-Object -First 1
} while ($null -eq $Conn -and (Get-Date) -lt $Deadline)

# --- Verify listener is up ---
if ($null -eq $Conn) {
  Write-Host ("ERROR: Nothing is listening on port {0}" -f $Port)
  Write-Host "Dashboard process did not bind before the startup deadline."
  exit 3
}

Write-Host ("Listener OK: {0}:{1} PID={2}" -f $Conn.LocalAddress, $Conn.LocalPort, $Conn.OwningProcess)
Write-Host ("Fast local URL: http://{0}:{1}/" -f $DashboardHost, $Port)

# --- Health check ---
try {
  $HealthUrl = ("http://{0}:{1}/health" -f $DashboardHost, $Port)
  $Resp = $null
  $HealthDeadline = (Get-Date).AddSeconds(20)
  do {
    try {
      $Resp = Invoke-RestMethod $HealthUrl -TimeoutSec 3
    } catch {
      Start-Sleep -Milliseconds 500
    }
  } while ($null -eq $Resp -and (Get-Date) -lt $HealthDeadline)
  if ($null -eq $Resp) {
    throw "health endpoint did not become ready within 20 seconds"
  }
  Write-Host ("Health OK: {0}" -f ($Resp | ConvertTo-Json -Depth 4))

  $WarmUrls = @(
    ("http://{0}:{1}/api/dashboard_meta?window=all" -f $DashboardHost, $Port),
    ("http://{0}:{1}/api/subaccounts?window=all" -f $DashboardHost, $Port),
    ("http://{0}:{1}/" -f $DashboardHost, $Port)
  )
  foreach ($WarmUrl in $WarmUrls) {
    Invoke-WebRequest -Uri $WarmUrl -UseBasicParsing -TimeoutSec 8 | Out-Null
  }
  Write-Host "Warm-up OK: root + dashboard APIs primed"

  $UiUrl = ("http://{0}:{1}/" -f $DashboardHost, $Port)
  Start-Process $UiUrl

} catch {
  Write-Host ("WARN: Health check failed: {0}" -f $_.Exception.Message)
}

exit 0
