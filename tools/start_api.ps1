# tools\start_api.ps1
# Kill anything listening on :5000 then start the Cockpit API (FastAPI via uvicorn).

Set-Location C:\flashback

# Kill listeners on port 5000
@(Get-NetTCPConnection -State Listen -LocalPort 5000 -ErrorAction SilentlyContinue |
  Select-Object -ExpandProperty OwningProcess -Unique) |
  ForEach-Object { Stop-Process -Id $_ -Force -ErrorAction SilentlyContinue }

# Logs
$stdout = "C:\flashback\state\api_stdout.log"
$stderr = "C:\flashback\state\api_stderr.log"
Remove-Item $stdout,$stderr -Force -ErrorAction SilentlyContinue

# Start API (new window so this terminal stays usable)
Start-Process -FilePath "python" -ArgumentList @(
  "-m","uvicorn",
  "app.api.cockpit_api:app",
  "--host","127.0.0.1",
  "--port","5000"
) -WorkingDirectory "C:\flashback" -WindowStyle Normal `
  -RedirectStandardOutput $stdout -RedirectStandardError $stderr

# Wait a moment then show listener + quick health check
Start-Sleep -Seconds 1
Get-NetTCPConnection -State Listen -LocalPort 5000 -ErrorAction SilentlyContinue |
  Select-Object LocalAddress,LocalPort,OwningProcess
