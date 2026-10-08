param(
    [string]$Pattern = '^TestGatewayPoolContinuousWait',
    [string[]]$Packages = @('./internal/service')
)
$ErrorActionPreference = 'Stop'
Push-Location (Join-Path $PSScriptRoot '../backend')
try {
    & go test -p 2 -tags unit -timeout 60s "-run=$Pattern" -count=1 @Packages
    if ($LASTEXITCODE -ne 0) { throw 'Gateway pool continuous wait checks failed' }
} finally {
    Pop-Location
}
