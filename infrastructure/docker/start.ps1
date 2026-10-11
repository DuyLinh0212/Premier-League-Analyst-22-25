param([switch]$Build)
$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
if (-not (Test-Path -LiteralPath '.env')) { throw 'Copy .env.example to .env and configure existing services.' }
function Invoke-TaskDocker { & docker @args; if ($LASTEXITCODE -ne 0) { throw "Docker failed: $LASTEXITCODE" } }
Invoke-TaskDocker info --format '{{.ServerVersion}}'
Invoke-TaskDocker compose --env-file .env config --quiet
if ($Build) { Invoke-TaskDocker compose --env-file .env --profile tools build runtime api web }
Invoke-TaskDocker compose --env-file .env --profile tools run --rm --no-deps --pull never publish
Invoke-TaskDocker compose --env-file .env up -d --no-build --pull never --wait api web
Write-Host 'Web ready. Airflow/Kafka/Hadoop/Neon PostgreSQL are external.'
