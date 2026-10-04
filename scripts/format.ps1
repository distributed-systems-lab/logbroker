[CmdletBinding()]
param(
    [ValidateSet('apply', 'check')]
    [string]$Mode = 'apply'
)

$ErrorActionPreference = 'Stop'
$Mode = $Mode.ToLowerInvariant()
$maven = Get-Command mvn -ErrorAction Stop
$repoRoot = Split-Path -Parent $PSScriptRoot
Push-Location -LiteralPath $repoRoot
try {
    # Native stderr may contain ordinary Java warnings; Maven's exit code is authoritative.
    $ErrorActionPreference = 'Continue'
    & $maven.Source -B "spotless:$Mode"
    $result = $LASTEXITCODE
} finally {
    Pop-Location
}
exit $result
