param(
    [string]$GoExecutable = 'go',
    [string]$OutputDirectory = (Join-Path ([IO.Path]::GetTempPath()) 'sub2api-fingerprint-offline'),
    [string]$TestPattern = '^Test(CodexFingerprint|CodexAccountIdentity|ApplyCodexFingerprint|StageCodexFingerprint|ApplyStagedCodexFingerprint|OpenAIWSHTTPBridgeLaterTurn429RetriesCurrentTurnOnReplacementAccount)'
)

$ErrorActionPreference = 'Stop'
$project = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$output = [IO.Path]::GetFullPath($OutputDirectory)
if ($output.Equals($project, [StringComparison]::OrdinalIgnoreCase) -or
    $output.StartsWith($project + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'Keep Linux test artifacts outside the Windows worktree.'
}
$null = New-Item -ItemType Directory -Path $output -Force
$binary = Join-Path $output 'codex-fingerprint.test'
$overrides = @{ GOOS = 'linux'; GOARCH = 'amd64'; CGO_ENABLED = '0'; GOTOOLCHAIN = 'local'; GOPROXY = 'off'; GOSUMDB = 'off'; GOWORK = 'off' }
$previous = @{}
foreach ($name in $overrides.Keys) {
    $previous[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
}

Push-Location (Join-Path $project 'backend')
try {
    foreach ($name in $overrides.Keys) {
        [Environment]::SetEnvironmentVariable($name, $overrides[$name], 'Process')
    }
    & $GoExecutable test -c -buildvcs=false -o $binary ./internal/service
    if ($LASTEXITCODE -ne 0) { throw 'Test compilation failed; use an installed Go version matching backend/go.mod.' }
    $linuxBinary = & wsl.exe --exec wslpath -u $binary
    if ($LASTEXITCODE -ne 0) { throw 'Cannot convert the test-binary path for WSL.' }
    $linuxRunner = & wsl.exe --exec wslpath -u (Join-Path $PSScriptRoot 'run-codex-fingerprint-offline.sh')
    if ($LASTEXITCODE -ne 0) { throw 'Cannot convert the isolation-runner path for WSL.' }
    & wsl.exe --exec sh $linuxRunner.Trim() $linuxBinary.Trim() $TestPattern
    if ($LASTEXITCODE -ne 0) { throw 'Offline fingerprint tests failed.' }
}
finally {
    Pop-Location
    foreach ($name in $previous.Keys) {
        [Environment]::SetEnvironmentVariable($name, $previous[$name], 'Process')
    }
}
