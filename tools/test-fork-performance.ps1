param(
    [string]$Benchmark = '^BenchmarkForkPerformance',
    [string]$Benchtime = '200ms',
    [int]$Count = 3
)
$ErrorActionPreference = 'Stop'
Push-Location (Join-Path $PSScriptRoot '../backend')
try {
    & go test -p 2 -tags unit -timeout 60s -run '^$' -bench $Benchmark -benchmem "-benchtime=$Benchtime" "-count=$Count" ./internal/service
    if ($LASTEXITCODE -ne 0) { throw 'Fork performance benchmark failed' }
} finally {
    Pop-Location
}
