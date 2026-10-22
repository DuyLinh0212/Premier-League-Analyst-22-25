param([switch]$NoBrowser)
$ErrorActionPreference = 'Stop'
$OutputEncoding = [Text.UTF8Encoding]::new($false)
try { [Console]::OutputEncoding = $OutputEncoding } catch { }
& (Join-Path $PSScriptRoot 'run_pipeline.ps1') -Action start
if ($LASTEXITCODE -ne 0) { throw 'Không khởi động được pipeline; xem log đã hiển thị.' }
Write-Host '[THÔNG TIN] Web truy vấn PostgreSQL: http://127.0.0.1:5173'
if (-not $NoBrowser) { Start-Process 'http://127.0.0.1:5173' }