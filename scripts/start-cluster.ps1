# Starts a 5-node cluster on this machine (in-memory storage, no Docker needed).
# Each node runs in the background and logs to logs\nX.log.
# Stop everything with: .\scripts\stop-cluster.ps1
#
# Usage:  .\scripts\start-cluster.ps1

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

# Always do a clean build. IDEs such as VS Code's Java extension also compile into
# target/classes, and a stale class file from them can end up in the jar.
Write-Host "Building..."
# Newer JVMs print harmless warnings on stderr. Windows PowerShell 5.1 treats native
# stderr as errors, so relax error handling for the build and filter those lines out.
$ErrorActionPreference = "Continue"
& .\mvnw.cmd -q -DskipTests clean package 2>&1 |
    ForEach-Object { "$_" } | Where-Object { $_ -notmatch '^WARNING|RemoteException' -and $_.Trim() -ne '' }
$buildExit = $LASTEXITCODE
$ErrorActionPreference = "Stop"
if ($buildExit -ne 0) { throw "Build failed" }

New-Item -ItemType Directory -Force logs | Out-Null
$pids = @()
foreach ($i in 1..5) {
    $env:NODE_ID = "n$i"
    $env:HTTP_PORT = "808$i"
    $env:GRPC_PORT = "909$i"
    $p = Start-Process java -ArgumentList "-jar", "target\raftkv-1.0.0.jar" `
        -RedirectStandardOutput "logs\n$i.log" -RedirectStandardError "logs\n$i.err.log" `
        -WindowStyle Hidden -PassThru
    $pids += $p.Id
    Write-Host "Started n$i (pid $($p.Id))  http://localhost:808$i"
}
$pids | Set-Content logs\pids.txt
Write-Host "`nWatch the election:  Get-Content logs\n1.log -Wait"
Write-Host "Cluster status:      curl.exe -s http://localhost:8081/raft/status"
