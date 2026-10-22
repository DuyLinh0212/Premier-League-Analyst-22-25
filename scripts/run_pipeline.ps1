param(
    [ValidateSet('setup','start','stop','status','run','dry-run')][string]$Action = 'status',
    [string]$RunId = '',
    [switch]$SkipPostgresSetup
)
$ErrorActionPreference = 'Stop'
$OutputEncoding = [Text.UTF8Encoding]::new($false)
try { [Console]::InputEncoding = $OutputEncoding; [Console]::OutputEncoding = $OutputEncoding } catch { }
$project = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
if ($Action -eq 'setup' -and -not $SkipPostgresSetup) {
    & (Join-Path $PSScriptRoot 'setup_pipeline_postgres.ps1')
    if ($LASTEXITCODE -ne 0) { throw 'Chưa hoàn tất cấu hình PostgreSQL.' }
}
$linuxProject = (@(& wsl.exe -d Ubuntu --exec wslpath -u $project.Replace('\','/')) -join '').Trim()
if ($LASTEXITCODE -ne 0) { throw 'Không xác định được dự án trong WSL.' }
$forwarded = @('env')
Get-ChildItem Env: | Where-Object { $_.Name -like 'PLCA_*' -or $_.Name -in @('GITHUB_SHA','GITHUB_RUN_ID','GITHUB_RUN_ATTEMPT') } | ForEach-Object { $forwarded += "$($_.Name)=$($_.Value)" }
$arguments = @('--cd',$linuxProject,'--exec') + $forwarded + @('/root/.venvs/plca-spark/bin/python','-m','src.pipeline.ops',$Action)
if ($RunId) { $arguments += @('--run-id',$RunId) }
& wsl.exe -d Ubuntu @arguments
exit $LASTEXITCODE
