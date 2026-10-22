param([string]$ApiBaseUrl = 'http://127.0.0.1:8000')
$ErrorActionPreference = 'Stop'
$OutputEncoding = [Text.UTF8Encoding]::new($false)
try { [Console]::OutputEncoding = $OutputEncoding } catch { }
$project = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$npm = (Get-Command npm.cmd -ErrorAction Stop).Source
$oldApiBaseUrl = $env:VITE_API_BASE_URL
$env:VITE_API_BASE_URL = $ApiBaseUrl
Push-Location -LiteralPath (Join-Path $project 'apps/analytics-web/web')
try {
    Write-Host '[THÔNG TIN] Đang cài thư viện Web theo package-lock.json...'
    & $npm ci --no-audit --no-fund
    if ($LASTEXITCODE -ne 0) { throw 'Không cài được thư viện Web.' }
    Write-Host '[THÔNG TIN] Đang build giao diện Web...'
    & $npm run build
    if ($LASTEXITCODE -ne 0) { throw 'Build Web thất bại.' }
} finally { $env:VITE_API_BASE_URL = $oldApiBaseUrl; Pop-Location }
