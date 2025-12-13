# Flashback — Clean Audit Zip Builder
# Creates a shareable zip WITHOUT runtime junk, secrets, git objects, or venv.
# Output: ./_audit/Flashback_AUDIT_YYYY-MM-DD_HHMM.zip

$ErrorActionPreference = "Stop"

# --- Repo root (this script assumes it's run from repo root) ---
$ROOT = (Get-Location).Path

# --- Output folder ---
$OUTDIR = Join-Path $ROOT "_audit"
New-Item -ItemType Directory -Force -Path $OUTDIR | Out-Null

$stamp = Get-Date -Format "yyyy-MM-dd_HHmm"
$zipName = "Flashback_AUDIT_$stamp.zip"
$zipPath = Join-Path $OUTDIR $zipName

# --- Exclusion rules (paths relative to repo root) ---
$exclude = @(
  ".git",
  ".github",
  ".venv",
  "venv",
  "__pycache__",
  "logs",
  "state",
  "state_RUNTIME_HOLD",
  "tmp",
  "app\state",
  "signals",
  ".env",
  ".env.local",
  ".env.prod",
  ".env.dev",
  "*.jsonl",
  "*.cursor",
  "*.db"
)

# --- Build file list ---
Write-Host "Building clean audit zip..."
Write-Host "Repo root: $ROOT"
Write-Host "Output:   $zipPath"
Write-Host ""

# Collect all files under repo root
$allFiles = Get-ChildItem -Path $ROOT -Recurse -File -Force |
  Where-Object {
    $rel = $_.FullName.Substring($ROOT.Length).TrimStart("\","/")

    # Exclude by directory prefix
    foreach ($ex in $exclude) {
      # directory exclusions
      if ($ex -notlike "*.*" -and $ex -notlike "*\**" -and $ex -notlike "*/*") {
        if ($rel -like "$ex\*" -or $rel -eq $ex) { return $false }
      }
    }

    # Exclude explicit filenames
    foreach ($ex in $exclude) {
      if ($ex -like "*.jsonl" -and $rel -like "*.jsonl") { return $false }
      if ($ex -like "*.cursor" -and $rel -like "*.cursor") { return $false }
      if ($ex -like "*.db" -and $rel -like "*.db") { return $false }
      if ($rel -ieq $ex) { return $false }
      if ($rel -like $ex -and $ex -like "*.*") { return $false }
    }

    return $true
  }

if (-not $allFiles -or $allFiles.Count -eq 0) {
  throw "No files selected for zip (exclusions too aggressive)."
}

Write-Host ("Selected files: " + $allFiles.Count)

# Remove old zip if exists
if (Test-Path $zipPath) { Remove-Item $zipPath -Force }

# --- Create zip ---
# Compress-Archive requires relative paths; we build a temp staging folder to preserve structure.
$stage = Join-Path $OUTDIR "_stage"
if (Test-Path $stage) { Remove-Item $stage -Recurse -Force }
New-Item -ItemType Directory -Force -Path $stage | Out-Null

foreach ($f in $allFiles) {
  $rel = $f.FullName.Substring($ROOT.Length).TrimStart("\","/")
  $dest = Join-Path $stage $rel
  $destDir = Split-Path $dest -Parent
  New-Item -ItemType Directory -Force -Path $destDir | Out-Null
  Copy-Item -Path $f.FullName -Destination $dest -Force
}

Compress-Archive -Path (Join-Path $stage "*") -DestinationPath $zipPath -Force
Remove-Item $stage -Recurse -Force

Write-Host ""
Write-Host "✅ Done."
Write-Host "ZIP created: $zipPath"
