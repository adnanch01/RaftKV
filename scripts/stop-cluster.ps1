# Stops the nodes started by start-cluster.ps1.
$root = Split-Path -Parent $PSScriptRoot
$pidFile = Join-Path $root "logs\pids.txt"
if (Test-Path $pidFile) {
    Get-Content $pidFile | ForEach-Object {
        try { Stop-Process -Id $_ -Force -ErrorAction Stop; Write-Host "Stopped pid $_" } catch {}
    }
    Remove-Item $pidFile
}
