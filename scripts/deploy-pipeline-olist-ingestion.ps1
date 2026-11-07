<#
.SYNOPSIS
    Deploy and Verify Arroyo Olist Historical Ingestion Pipeline (Story 3.10)
.DESCRIPTION
    Invokes deploy-pipeline-olist-ingestion.py to verify broker consumer groups,
    deploy Arroyo streaming SQL job, and assert 2PC Delta Lake sink.
#>
[CmdletBinding()]
param (
    [string]$ArroyoUrl = $env:ARROYO_API_URL,
    [string]$IggyUrl = $env:STREAMING_HTTP_URL,
    [string]$DeltaDir = $env:DELTA_STORAGE_PATH,
    [string]$Secret = $env:IGGY_HTTP_JWT_ENCODING_SECRET,
    [string]$Account = "svc_ferrous_streaming",
    [switch]$NoDevSecret,
    [switch]$NoStrict,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PyScript = Join-Path $ScriptDir "deploy-pipeline-olist-ingestion.py"

$ArgsList = @()
if ($ArroyoUrl) { $ArgsList += "--arroyo-url", $ArroyoUrl }
if ($IggyUrl) { $ArgsList += "--iggy-url", $IggyUrl }
if ($DeltaDir) { $ArgsList += "--delta-dir", $DeltaDir }
if ($Secret) { $ArgsList += "--secret", $Secret }
if ($Account) { $ArgsList += "--account", $Account }
if ($NoDevSecret) { $ArgsList += "--no-dev-secret" }
if ($NoStrict) { $ArgsList += "--no-strict" }
if ($DryRun) { $ArgsList += "--dry-run" }

& python $PyScript @ArgsList
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

