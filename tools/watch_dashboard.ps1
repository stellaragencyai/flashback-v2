param()

$paths = @(
  "C:\flashback\app\dashboard\templates",
  "C:\flashback\app\dashboard"
)

function Kill-Port5000 {
  $line = (netstat -ano | Select-String ":5000" | Select-String "LISTENING" | Select-Object -First 1)
  if ($line) {
    $pid = ($line -split "\s+")[-1]
    if ($pid -match "^\d+$") {
      taskkill /PID $pid /F | Out-Null
    }
  }
}

function Start-Dashboard {
  Start-Process -FilePath "python" -ArgumentList ".\app\dashboard\dashboard_server.py" -WorkingDirectory "C:\flashback" | Out-Null
}

Kill-Port5000
Start-Dashboard

$watchers = @()
foreach ($p in $paths) {
  $w = New-Object System.IO.FileSystemWatcher
  $w.Path = $p
  $w.IncludeSubdirectories = $true
  $w.Filter = "*.*"
  $w.EnableRaisingEvents = $true
  $watchers += $w
}

$action = {
  Start-Sleep -Milliseconds 250
  Kill-Port5000
  Start-Dashboard
  Write-Host ("Restarted dashboard at " + (Get-Date))
}

foreach ($w in $watchers) {
  Register-ObjectEvent $w Changed -Action $action | Out-Null
  Register-ObjectEvent $w Created -Action $action | Out-Null
  Register-ObjectEvent $w Deleted -Action $action | Out-Null
  Register-ObjectEvent $w Renamed -Action $action | Out-Null
}

Write-Host "Watching dashboard files. Ctrl+C to stop."
while ($true) { Start-Sleep -Seconds 2 }
