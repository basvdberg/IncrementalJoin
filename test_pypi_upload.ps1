# Script to test uploading to TestPyPI
# Usage: .\test_pypi_upload.ps1

param(
    [string]$PythonExe = "python"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Ensure we're in repo root
$repoRoot = Resolve-Path "."
Set-Location $repoRoot

Write-Host "Building package..." -ForegroundColor Cyan
& $PythonExe -m build

if ($LASTEXITCODE -ne 0) {
    Write-Host "Build failed!" -ForegroundColor Red
    exit 1
}

Write-Host "`nBuild successful! Files in dist/:" -ForegroundColor Green
Get-ChildItem dist\ | Format-Table Name, Length

Write-Host "`nUploading to TestPyPI..." -ForegroundColor Cyan
Write-Host "You will be prompted for your TestPyPI credentials." -ForegroundColor Yellow
Write-Host "Username: __token__" -ForegroundColor Yellow
Write-Host "Password: Your TestPyPI API token (starts with pypi-)" -ForegroundColor Yellow
Write-Host "`nTo get a TestPyPI token:" -ForegroundColor Yellow
Write-Host "1. Go to https://test.pypi.org/manage/account/token/" -ForegroundColor Yellow
Write-Host "2. Create a new API token" -ForegroundColor Yellow
Write-Host "3. Copy the token and use it as the password" -ForegroundColor Yellow
Write-Host ""

& $PythonExe -m twine upload --repository testpypi dist/*

if ($LASTEXITCODE -eq 0) {
    Write-Host "`nUpload successful!" -ForegroundColor Green
    Write-Host "You can test installing with:" -ForegroundColor Cyan
    Write-Host "pip install -i https://test.pypi.org/simple/ inc-join" -ForegroundColor White
} else {
    Write-Host "`nUpload failed!" -ForegroundColor Red
    exit 1
}
