param(
  [string]$OnlyLabels = "",
  [switch]$NoStart,
  [int]$IntervalSec = 0
)

$ErrorActionPreference = "Stop"

$script:OrchStarted = $false


function RunPy([string]$mod) {
  Write-Host "==> python -m $mod"
  python -m $mod
  if ($LASTEXITCODE -ne 0) { throw "FAILED: $mod (exit $LASTEXITCODE)" }
}

function PrintStatus() {
  python -c "import json; from pathlib import Path;
snap=json.loads(Path(r'state\fleet_snapshot.json').read_text(encoding='utf-8', errors='ignore'));
deg=json.loads(Path(r'state\fleet_degraded.json').read_text(encoding='utf-8', errors='ignore'));
subs=(snap.get('fleet') or {}).get('subs') or {};
labels=sorted(list(subs.keys()));
print('\n=== FLEET STATUS ===');
print('fleet_mode=', snap.get('fleet_mode'));
print('count=', (snap.get('fleet') or {}).get('count'));
print('degraded_labels=', deg.get('labels_degraded') or []);
print('');
for lbl in labels:
    v=subs.get(lbl) or {};
    print(lbl,
          'should_run=', v.get('should_run'),
          'effective_should_run=', v.get('effective_should_run'),
          'alive=', v.get('alive'),
          'pid=', v.get('pid'),
          'restart_count=', v.get('restart_count'),
          'blocked=', v.get('blocked'),
          'backoff_sec=', v.get('backoff_sec'))"
}

function OnePass() {
  if ($OnlyLabels -ne "") {
    $Env:ORCH_ONLY_LABELS = $OnlyLabels
    Write-Host "ORCH_ONLY_LABELS=$OnlyLabels"
  } else {
    Remove-Item Env:ORCH_ONLY_LABELS -ErrorAction SilentlyContinue | Out-Null
  }

  RunPy "app.ops.ops_snapshot_tick"
  if (-not $NoStart) {
    if (-not $script:OrchStarted) {
      RunPy "app.ops.orchestrator_v1"
      $script:OrchStarted = $true
    }
  }
RunPy "app.ops.orchestrator_watchdog"
  RunPy "app.ops.ops_snapshot_tick"
  RunPy "app.ops.fleet_snapshot_tick"
  RunPy "app.ops.fleet_degraded"

  PrintStatus
}

if ($IntervalSec -gt 0) {
  Write-Host "fleet_up loop running every $IntervalSec sec. Ctrl+C to stop."
  while ($true) {
    OnePass
    Start-Sleep -Seconds $IntervalSec
  }
} else {
  OnePass
}
