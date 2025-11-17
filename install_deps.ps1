param(
    [string]$PythonExe = "python"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Ensure we're in repo root (this script should live here)
$repoRoot = Resolve-Path "."
Set-Location $repoRoot

& $PythonExe -m pip install --upgrade pip
& $PythonExe -m pip install -e .

Write-Host "Dependencies installed from pyproject.toml" -ForegroundColor Green



