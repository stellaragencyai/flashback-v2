param(
    [switch]$DryRun
)

Write-Host "=== Flashback: System Prep & Clean ===" -ForegroundColor Cyan
Write-Host "DryRun mode: $DryRun`n"

function KillIfRunning {
    param(
        [Parameter(Mandatory=$true, Position=0)]
        [string]$Name
    )
    try {
        $procs = Get-Process -Name $Name -ErrorAction SilentlyContinue
        if ($procs) {
            if ($DryRun) {
                Write-Host "[DRY] Would kill process: $Name (count=$($procs.Count))"
            } else {
                Write-Host "[KILL] Stopping process: $Name (count=$($procs.Count))"
                $procs | Stop-Process -Force -ErrorAction SilentlyContinue
            }
        }
    } catch {
        # don't care
    }
}

function ClearPathSafe {
    param(
        [Parameter(Mandatory=$true, Position=0)]
        [string]$InputPath
    )

    if (-not (Test-Path $InputPath)) {
        return
    }

    Write-Host "[CLEAN] $InputPath"
    try {
        if ($DryRun) {
            Get-ChildItem $InputPath -Recurse -Force -ErrorAction SilentlyContinue |
                Select-Object FullName
        } else {
            Get-ChildItem $InputPath -Recurse -Force -ErrorAction SilentlyContinue |
                Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
        }
    } catch {
        Write-Host "  -> Error while cleaning ${InputPath}: $($_.Exception.Message)" -ForegroundColor Yellow
    }
}

Write-Host "Step 1: Killing noisy background apps..." -ForegroundColor Green

$ramHogs = @(
    "OneDrive",
    "Teams",
    "Discord",
    "Steam",
    "EpicGamesLauncher",
    "Dropbox",
    "Spotify",
    "Telegram",
    "Slack",
    "Zoom"
)

foreach ($name in $ramHogs) {
    KillIfRunning $name
}

Write-Host "`nStep 2: Cleaning temp folders..." -ForegroundColor Green

$tempPaths = @(
    "$env:TEMP\*",
    "C:\Windows\Temp\*"
)

foreach ($p in $tempPaths) {
    ClearPathSafe $p
}

Write-Host "`nStep 3: Emptying Recycle Bin..." -ForegroundColor Green
try {
    if ($DryRun) {
        Write-Host "[DRY] Would clear Recycle Bin"
    } else {
        Clear-RecycleBin -Force -ErrorAction SilentlyContinue
    }
} catch {
    Write-Host "  -> Error clearing Recycle Bin: $($_.Exception.Message)" -ForegroundColor Yellow
}

Write-Host "`nStep 4: Disk free space snapshot:" -ForegroundColor Green
Get-PSDrive -PSProvider FileSystem |
    Select-Object Name,
        @{n="FreeGB";e={[math]::Round($_.Free/1GB,2)}},
        @{n="UsedGB";e={[math]::Round(($_.Used)/1GB,2)}} |
    Format-Table -AutoSize

Write-Host "`nStep 5: Top 10 RAM-hungry processes:" -ForegroundColor Green
Get-Process |
    Sort-Object -Property WS -Descending |
    Select-Object -First 10 `
        @{n="Name";e={$_.ProcessName}},
        @{n="WS_MB";e={[math]::Round($_.WS/1MB,1)}} |
    Format-Table -AutoSize

Write-Host "`n=== Flashback prep complete. Restart VS Code & trading stack if needed. ===" -ForegroundColor Cyan
