# Script to test installing from TestPyPI
# Usage: .\test_pypi_install.ps1

param(
    [string]$PythonExe = "python"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Write-Host "Installing incremental-join from TestPyPI..." -ForegroundColor Cyan
Write-Host "This will install the package from TestPyPI to verify it works correctly." -ForegroundColor Yellow
Write-Host ""

# Install from TestPyPI (with fallback to regular PyPI for dependencies)
& $PythonExe -m pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ incremental-join

if ($LASTEXITCODE -eq 0) {
    Write-Host "`nInstallation successful!" -ForegroundColor Green
    Write-Host "Testing import..." -ForegroundColor Cyan
    
    & $PythonExe -c "from src.inc_join import inc_join; print('Import successful!')"
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "`nPackage works correctly!" -ForegroundColor Green
    } else {
        Write-Host "`nImport test failed!" -ForegroundColor Red
        exit 1
    }
} else {
    Write-Host "`nInstallation failed!" -ForegroundColor Red
    exit 1
}

