param(
  [int]$IntervalSec = 5,
  [string]$OnlyLabels = ""
)

$ErrorActionPreference = "Stop"

if ($OnlyLabels -ne "") {
  $Env:ORCH_ONLY_LABELS = $OnlyLabels
} else {
  Remove-Item Env:ORCH_ONLY_LABELS -ErrorAction SilentlyContinue | Out-Null
}

Write-Host "watchdog_loop running every $IntervalSec sec. Ctrl+C to stop."
while ($true) {
  python -m app.ops.orchestrator_watchdog | Out-Host
  Start-Sleep -Seconds $IntervalSec
}
