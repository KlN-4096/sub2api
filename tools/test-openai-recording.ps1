param([switch]$Race)
$ErrorActionPreference = 'Stop'
$backend = Join-Path $PSScriptRoot '../backend'
Push-Location $backend
try {
    $goArgs = @('test', '-p', '2', '-timeout', '60s', '-count=1', '-tags', 'unit',
        '-run', '^Test(Recorder|OpenAIRecording)', './internal/pkg/upstreamrecord', './internal/service', './cmd/recording-export')
    if ($Race) { $goArgs = @('test', '-race') + $goArgs[1..($goArgs.Length - 1)] }
    & go @goArgs
    if ($LASTEXITCODE -ne 0) { throw 'OpenAI recording tests failed' }
} finally {
    Pop-Location
}
