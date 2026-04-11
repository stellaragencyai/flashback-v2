Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$VenvPy = Join-Path $RepoRoot ".venv\Scripts\python.exe"
$VenvDir = Join-Path $RepoRoot ".venv"
$SitePackages = Join-Path $VenvDir "Lib\site-packages"
$SubaccountsPath = Join-Path $RepoRoot "config\subaccounts.yaml"
$StateDir = Join-Path $RepoRoot "state"
$LogDir = Join-Path $StateDir "orchestrator_logs"
$StdOut = Join-Path $StateDir "orchestrator_v2_stdout.log"
$StdErr = Join-Path $StateDir "orchestrator_v2_stderr.log"

function Stop-FlashbackProcessTree {
  param(
    [Parameter(Mandatory = $true)]
    [int]$ProcessId,
    [Parameter(Mandatory = $true)]
    [string]$Reason
  )

  Write-Host ("Stopping {0} PID={1}" -f $Reason, $ProcessId)
  try {
    & taskkill /PID $ProcessId /T /F | Out-Null
  } catch {
    Stop-Process -Id $ProcessId -Force -ErrorAction SilentlyContinue
  }
}

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

if (-not (Test-Path $SubaccountsPath)) {
  throw "Missing config/subaccounts.yaml at $SubaccountsPath"
}

New-Item -ItemType Directory -Force -Path $StateDir | Out-Null
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
Remove-Item -Force -ErrorAction SilentlyContinue $StdOut, $StdErr

$env:FLASHBACK_SUBACCOUNTS_PATH = $SubaccountsPath
$labelsText = @'
from pathlib import Path
import os
import yaml

cfg_path = Path(os.environ["FLASHBACK_SUBACCOUNTS_PATH"])
cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
labels = []

accounts = cfg.get("accounts")
if isinstance(accounts, list):
    for acc in accounts:
        if not isinstance(acc, dict):
            continue
        label = str(acc.get("account_label") or "").strip()
        if not label:
            continue
        if bool(acc.get("enabled", True)) and bool(acc.get("enable_ai_stack", False)):
            labels.append(label)
else:
    for key, value in cfg.items():
        if key in ("version", "notes", "legacy"):
            continue
        if not isinstance(value, dict):
            continue
        label = str(key).strip()
        if not label:
            continue
        if bool(value.get("enabled", True)) and bool(value.get("enable_ai_stack", False)):
            labels.append(label)

print("\n".join(labels))
'@ | & $RuntimePy -
Remove-Item Env:FLASHBACK_SUBACCOUNTS_PATH -ErrorAction SilentlyContinue

$labels = @(
  $labelsText -split "\r?\n" |
  ForEach-Object { $_.Trim() } |
  Where-Object { $_ }
)
if ($labels.Count -eq 0) {
  throw "No enabled AI-stack labels found in $SubaccountsPath"
}

$labelArg = ($labels -join ",")

Write-Host ("RepoRoot: {0}" -f $RepoRoot)
Write-Host ("Using runtime python: {0}" -f $RuntimePy)
Write-Host ("Launching labels: {0}" -f $labelArg)

$orchestratorPattern = "app\\ops\\orchestrator_v2\.py"
$supervisorPattern = [regex]::Escape((Join-Path $RepoRoot "app\bots\supervisor_ai_stack.py"))
$workerPatterns = @(
  [regex]::Escape((Join-Path $RepoRoot "app\core\ws_switchboard.py")),
  [regex]::Escape((Join-Path $RepoRoot "app\bots\executor_v2.py")),
  [regex]::Escape((Join-Path $RepoRoot "app\bots\tp_sl_manager.py")),
  [regex]::Escape((Join-Path $RepoRoot "app\bots\ai_pilot.py")),
  [regex]::Escape((Join-Path $RepoRoot "app\bots\ai_action_router.py")),
  [regex]::Escape((Join-Path $RepoRoot "app\bots\risk_daemon.py")),
  [regex]::Escape((Join-Path $RepoRoot "app\bots\trade_outcome_recorder.py")),
  [regex]::Escape((Join-Path $RepoRoot "app\sim\paper_price_feeder.py"))
)
$workerPattern = ($workerPatterns -join "|")

$processes = @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue)

$existing = $processes |
  Where-Object {
    $_.CommandLine -and
    $_.CommandLine -match $orchestratorPattern
  }

if ($existing) {
  Write-Host "Stopping existing orchestrator_v2 processes..."
  $existing | ForEach-Object {
    Stop-FlashbackProcessTree -ProcessId $_.ProcessId -Reason "orchestrator_v2"
  }
  Start-Sleep -Seconds 1
}

$existingSupervisors = $processes |
  Where-Object {
    $_.CommandLine -and
    $_.CommandLine -match $supervisorPattern
  }

if ($existingSupervisors) {
  Write-Host "Stopping existing supervisor_ai_stack processes..."
  $existingSupervisors | ForEach-Object {
    Stop-FlashbackProcessTree -ProcessId $_.ProcessId -Reason "supervisor_ai_stack"
  }
  Start-Sleep -Seconds 1
}

$staleWorkers = $processes |
  Where-Object {
    $_.CommandLine -and
    $_.CommandLine -match $workerPattern
  }

if ($staleWorkers) {
  Write-Host "Stopping stray worker processes before restart..."
  $staleWorkers | Sort-Object ProcessId -Unique | ForEach-Object {
    Stop-FlashbackProcessTree -ProcessId $_.ProcessId -Reason "stale worker"
  }
  Start-Sleep -Seconds 1
}

$staleLogPaths = @(
  (Join-Path $LogDir "System.Object[].stdout.log"),
  (Join-Path $LogDir "System.Object[].stderr.log")
)
$staleLockPaths = @(
  (Join-Path $StateDir "orchestrator_locks\orchestrator_v2_system.object[].lock")
)
foreach ($path in $staleLogPaths + $staleLockPaths) {
  if (Test-Path -LiteralPath $path) {
    Remove-Item -Force -ErrorAction SilentlyContinue -LiteralPath $path
  }
}

$opsSnapshot = Join-Path $StateDir "ops_snapshot.json"
if (Test-Path $opsSnapshot) {
  $env:FLASHBACK_OPS_SNAPSHOT = $opsSnapshot
  @'
from pathlib import Path
import json
import os

path = Path(os.environ["FLASHBACK_OPS_SNAPSHOT"])
try:
    data = json.loads(path.read_text(encoding="utf-8"))
except Exception:
    raise SystemExit(0)

components = data.get("components")
if not isinstance(components, dict):
    raise SystemExit(0)

filtered = {
    key: value
    for key, value in components.items()
    if str((value or {}).get("account_label", "")).strip() != "System.Object[]"
}

if len(filtered) != len(components):
    data["components"] = filtered
    path.write_text(json.dumps(data, separators=(",", ":")), encoding="utf-8")
'@ | & $RuntimePy -
  Remove-Item Env:FLASHBACK_OPS_SNAPSHOT -ErrorAction SilentlyContinue
}

$proc = Start-Process -FilePath $RuntimePy `
  -WorkingDirectory $RepoRoot `
  -ArgumentList @(
    "-u",
    "app\ops\orchestrator_v2.py",
    "--labels", $labelArg,
    "--restart",
    "--health",
    "--health-every", "30",
    "--status-every", "5"
  ) `
  -WindowStyle Normal `
  -RedirectStandardOutput $StdOut `
  -RedirectStandardError $StdErr `
  -PassThru

Start-Sleep -Seconds 2

Write-Host ("Started orchestrator_v2 PID={0}" -f $proc.Id)
Write-Host ("stdout: {0}" -f $StdOut)
Write-Host ("stderr: {0}" -f $StdErr)

Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
  Where-Object {
    $_.CommandLine -and
    $_.CommandLine -match "app\\ops\\orchestrator_v2\.py"
  } |
  Select-Object ProcessId, CommandLine |
  Format-Table -AutoSize
