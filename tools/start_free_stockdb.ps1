$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$stockDbRoot = Join-Path $ProjectRoot "runtime\free-stockdb"
$stockDbExe = Join-Path $stockDbRoot "stockdb.exe"
$stockDbConfig = Join-Path $stockDbRoot "stockdb.conf"
$stockDbData = Join-Path $stockDbRoot "data"

if (-not (Test-Path -LiteralPath $stockDbExe -PathType Leaf)) {
    Write-Warning "free-stockdb is not installed; expected: $stockDbExe"
    exit 0
}
if (-not (Test-Path -LiteralPath $stockDbConfig -PathType Leaf)) {
    Write-Warning "free-stockdb config is missing: $stockDbConfig"
    exit 0
}
if (-not (Test-Path -LiteralPath $stockDbData -PathType Container)) {
    Write-Warning "free-stockdb data directory is missing: $stockDbData"
    exit 0
}

function Test-FreeStockDbPort {
    $client = [System.Net.Sockets.TcpClient]::new()
    try {
        $pending = $client.ConnectAsync("127.0.0.1", 7899)
        return $pending.Wait(250) -and $client.Connected
    }
    catch {
        return $false
    }
    finally {
        $client.Dispose()
    }
}

if (Test-FreeStockDbPort) {
    Write-Host "free-stockdb is already running on 127.0.0.1:7899"
    exit 0
}

try {
    Start-Process -FilePath $stockDbExe -WorkingDirectory $stockDbRoot -WindowStyle Hidden
    $deadline = [DateTime]::UtcNow.AddSeconds(10)
    do {
        Start-Sleep -Milliseconds 250
        if (Test-FreeStockDbPort) {
            Write-Host "free-stockdb started on 127.0.0.1:7899"
            exit 0
        }
    } while ([DateTime]::UtcNow -lt $deadline)
    Write-Warning "free-stockdb did not listen on 127.0.0.1:7899 within 10 seconds; QuantMaster will continue with fallback sources"
}
catch {
    Write-Warning "free-stockdb could not be started: $($_.Exception.Message)"
}

exit 0
