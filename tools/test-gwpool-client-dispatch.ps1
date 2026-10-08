param(
    [string]$Pattern = 'GatewayPool|GWPool|Gwpool',
    [string]$OutputDirectory = (Join-Path $env:TEMP 'sub2api-client-dispatch-tests')
)
$ErrorActionPreference = 'Stop'
$backend = Join-Path $PSScriptRoot '../backend'
$OutputDirectory = [System.IO.Path]::GetFullPath($OutputDirectory)
New-Item -ItemType Directory -Force -Path $OutputDirectory | Out-Null
$binary = Join-Path $OutputDirectory 'service.test.exe'
Push-Location $backend
try {
    & go test -p 2 -tags unit -c -o $binary ./internal/service
    if ($LASTEXITCODE -ne 0) { throw 'Service test compilation failed' }
    Set-Location (Join-Path $backend 'internal/service')
    $names = @(& $binary "-test.list=$Pattern" | Where-Object { $_ -match '^Test\w+$' } | Sort-Object)
    if ($names.Count -eq 0) { throw 'No matching tests' }
    $batchSize = 8
    $failures = 0
    for ($offset = 0; $offset -lt $names.Count; $offset += $batchSize) {
        $last = [Math]::Min($offset + $batchSize - 1, $names.Count - 1)
        $selection = '^(' + (($names[$offset..$last] | ForEach-Object { [regex]::Escape($_) }) -join '|') + ')$'
        $log = Join-Path $OutputDirectory ("batch-{0:D3}.log" -f ($offset / $batchSize))
        & $binary '-test.timeout=60s' "-test.run=$selection" '-test.v' *> $log
        $code = $LASTEXITCODE
        Write-Host ("Tests {0}-{1}/{2}: exit {3}; {4}" -f ($offset+1), ($last+1), $names.Count, $code, $log)
        if ($code -ne 0) { $failures++ }
    }
    if ($failures -ne 0) { throw "$failures gateway test batches failed" }
} finally {
    Pop-Location
}
