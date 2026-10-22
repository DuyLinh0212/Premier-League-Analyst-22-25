# Non-mutating regression check for Windows PowerShell 5.1 and PowerShell 7.
$ErrorActionPreference = 'Stop'
$OutputEncoding = [Text.UTF8Encoding]::new($false)
try { [Console]::OutputEncoding = $OutputEncoding } catch { }
$project = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '../../..')).Path
foreach ($name in @('setup_pipeline_postgres.ps1','run_pipeline.ps1','build_pipeline_web.ps1')) {
    $file = Join-Path $project ('scripts/' + $name)
    $tokens = $null; $parseErrors = $null
    [void][Management.Automation.Language.Parser]::ParseFile($file, [ref]$tokens, [ref]$parseErrors)
    if ($parseErrors.Count) { throw ($parseErrors | Out-String) }
    $bytes = [IO.File]::ReadAllBytes($file)
    if ($bytes[0] -ne 239 -or $bytes[1] -ne 187 -or $bytes[2] -ne 191) { throw "Missing UTF-8 BOM: $name" }
}
$linux = (@(& wsl.exe -d Ubuntu --exec wslpath -u $project.Replace('\','/')) -join '').Trim()
if ($LASTEXITCODE -ne 0) { throw 'wslpath failed' }
$temporary = '/tmp/plca-setup-bridge-' + [Guid]::NewGuid().ToString('N') + '.json'
try {
    $fixture = '{"port":5432,"writer":{"user":"plca_writer","password":"test_only_writer"},"reader":{"user":"plca_reader","password":"test_only_reader"},"airflow":{"user":"plca_airflow","password":"test_only_airflow"}}'
    $fixture | & wsl.exe -d Ubuntu --cd $linux --exec python3 -m src.pipeline.postgres_setup write --file $temporary
    if ($LASTEXITCODE -ne 0) { throw 'WSL write bridge failed' }
    $roundtrip = @(& wsl.exe -d Ubuntu --cd $linux --exec python3 -m src.pipeline.postgres_setup read --file $temporary) -join ''
    if ($LASTEXITCODE -ne 0) { throw 'WSL read bridge failed' }
    $value = $roundtrip | ConvertFrom-Json
    if ($value.writer.password -ne 'test_only_writer' -or $value.port -ne 5432) { throw 'JSON did not round-trip' }
    $network = @(& wsl.exe -d Ubuntu --cd $linux --exec python3 -m src.pipeline.postgres_setup network) -join ''
    if ($LASTEXITCODE -ne 0 -or $network -notmatch '^\d+\.\d+\.\d+\.\d+/\d+$') { throw 'WSL subnet check failed' }
    Write-Output ('PASS: PowerShell ' + $PSVersionTable.PSVersion.ToString() + ', parser, UTF-8 BOM, WSL read/write and subnet')
} finally {
    & wsl.exe -d Ubuntu --exec rm -f -- $temporary
}
