# Flashback — Windows Deployment Quickstart

This is a template guide for running Flashback in a semi-permanent way on Windows.

## 1. Prepare the environment

From a PowerShell window:

```powershell
cd C:\Path\To\Flashback
.\scripts\bootstrap_env.ps1
.\.venv\Scripts\Activate.ps1
python tools\validate_config.py
