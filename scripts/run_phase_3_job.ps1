# Flashback — Phase 3 Daily Job
# Usage:
#   powershell -ExecutionPolicy Bypass -File scripts\run_phase_3_job.ps1
#   powershell -ExecutionPolicy Bypass -File scripts\run_phase_3_job.ps1 -Strict

param(
  [switch]$Strict
)

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "🧠 Phase 3 Job — AI State Engine" -ForegroundColor Cyan
Write-Host ""

# 0) Ensure state files exist
if (-not (Test-Path "state")) { New-Item -ItemType Directory -Path "state" | Out-Null }
if (-not (Test-Path "state\features_trades.jsonl")) { New-Item -ItemType File -Path "state\features_trades.jsonl" | Out-Null }
if (-not (Test-Path "state\feature_store.jsonl")) { New-Item -ItemType File -Path "state\feature_store.jsonl" | Out-Null }

# 1) Seed minimal rows if empty (DEV bootstrap)
$ftSize = (Get-Item "state\features_trades.jsonl").Length
$fsSize = (Get-Item "state\feature_store.jsonl").Length

if (($ftSize -eq 0) -or ($fsSize -eq 0)) {
  Write-Host "🧪 State files empty → seeding minimal rows for validation..." -ForegroundColor Yellow
  python -m app.tools.seed_ai_state --n 5
  if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Seeding failed." -ForegroundColor Red
    exit 2
  }
}

# 2) Run health check
if ($Strict) {
  Write-Host "🔒 Running STRICT health thresholds..." -ForegroundColor Cyan
  python -m app.tools.ai_state_health --min-total 50 --min-per-strategy 10 --max-age-hours 72
} else {
  Write-Host "✅ Running bootstrap health thresholds..." -ForegroundColor Green
  python -m app.tools.ai_state_health --min-total 5 --min-per-strategy 1 --max-age-hours 9999
}

$code = $LASTEXITCODE

Write-Host ""
Write-Host "Exit code: $code"
Write-Host ""

if ($code -ge 2) {
  Write-Host "❌ Phase 3 FAIL — do not proceed." -ForegroundColor Red
  exit 2
}

if ($code -eq 1) {
  Write-Host "⚠️ Phase 3 WARN — proceed carefully." -ForegroundColor Yellow
  exit 1
}

Write-Host "✅ Phase 3 PASS — safe to proceed." -ForegroundColor Green
exit 0
