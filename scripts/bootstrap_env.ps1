param(
    [switch]$RecreateVenv
)

Write-Host "=== Flashback Bootstrap ===" -ForegroundColor Cyan

# Resolve project root (this script expected inside Flashback/scripts/)
$scriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectDir = Resolve-Path (Join-Path $scriptDir "..")
Set-Location $projectDir

Write-Host "Project root: $projectDir"

$venvPath   = Join-Path $projectDir ".venv"
$venvPython = Join-Path $venvPath "Scripts\python.exe"

if ($RecreateVenv -and (Test-Path $venvPath)) {
    Write-Host "[BOOT] Recreating .venv (per --RecreateVenv)" -ForegroundColor Yellow
    Remove-Item -Recurse -Force $venvPath
}

if (!(Test-Path $venvPath)) {
    Write-Host "[BOOT] Creating virtual environment at .venv" -ForegroundColor Green
    python -m venv .venv
    if (!(Test-Path $venvPython)) {
        Write-Host "[ERROR] Failed to create venv or python.exe not found in .venv" -ForegroundColor Red
        exit 1
    }
} else {
    Write-Host "[BOOT] Existing .venv found, skipping creation." -ForegroundColor DarkGray
}

# Install / upgrade dependencies
if (!(Test-Path "requirements.txt")) {
    Write-Host "[WARN] requirements.txt not found. Skipping pip install." -ForegroundColor Yellow
} else {
    Write-Host "[BOOT] Installing requirements into .venv" -ForegroundColor Green
    & $venvPython -m pip install --upgrade pip
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[ERROR] Failed to upgrade pip." -ForegroundColor Red
        exit 1
    }

    & $venvPython -m pip install -r requirements.txt
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[ERROR] Failed to install requirements.txt." -ForegroundColor Red
        exit 1
    }
}

# .env handling
$envFile        = Join-Path $projectDir ".env"
$envExampleFile = Join-Path $projectDir ".env.example"

if (!(Test-Path $envFile) -and (Test-Path $envExampleFile)) {
    Write-Host "[BOOT] .env not found, copying from .env.example" -ForegroundColor Green
    Copy-Item $envExampleFile $envFile
} elseif (!(Test-Path $envFile)) {
    Write-Host "[WARN] .env not found and no .env.example present. You must create .env manually." -ForegroundColor Yellow
} else {
    Write-Host "[BOOT] .env already exists, leaving it alone." -ForegroundColor DarkGray
}

# Sanity import check
Write-Host "[BOOT] Running sanity import check..." -ForegroundColor Green

$sanityCode = @'
from pathlib import Path

try:
    from app.core import flashback_common  # type: ignore
    from app.core import bus_types  # type: ignore
    print("FLASHBACK_BOOTSTRAP_OK")
except Exception as exc:
    print("FLASHBACK_BOOTSTRAP_FAIL:", repr(exc))
    raise
'@

$sanityCode | & $venvPython -

if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Sanity import check failed. See message above." -ForegroundColor Red
    exit 1
}

Write-Host "`n=== Flashback Bootstrap COMPLETE ===" -ForegroundColor Cyan
Write-Host "To activate venv in this shell:"
Write-Host "    .\.venv\Scripts\Activate.ps1" -ForegroundColor Yellow
Write-Host "Then you can run:"
Write-Host "    python tools/validate_config.py" -ForegroundColor Yellow
