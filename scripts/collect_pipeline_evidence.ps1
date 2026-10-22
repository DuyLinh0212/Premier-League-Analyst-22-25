param([Parameter(Mandatory=$true)][string]$RunId)
$ErrorActionPreference = 'Stop'
$OutputEncoding = [Text.UTF8Encoding]::new($false)
try { [Console]::OutputEncoding = $OutputEncoding } catch { }
$project=(Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$linux=(@(& wsl.exe -d Ubuntu --exec wslpath -u $project.Replace('\','/')) -join '').Trim()
if ($LASTEXITCODE -ne 0) { throw 'Không tìm thấy WSL project.' }
& wsl.exe -d Ubuntu --cd $linux --exec /root/.venvs/plca-spark/bin/python -m src.pipeline.evidence --run-id $RunId
exit $LASTEXITCODE
