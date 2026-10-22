param(
    [ValidateSet('setup','start','stop','status')][string]$Action = 'setup',
    [switch]$UseGitCredentialManager,
    [string]$RunnerDirectory = (Join-Path $env:LOCALAPPDATA 'PremierLeaguePipeline\github-runner'),
    [string]$Repository = 'DuyLinh0212/Premier-League-Analyst-22-25'
)
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
$OutputEncoding = [Text.UTF8Encoding]::new($false)
try { [Console]::OutputEncoding = $OutputEncoding } catch { }
$RunnerDirectory = [IO.Path]::GetFullPath($RunnerDirectory)
$pidFile = Join-Path $RunnerDirectory 'plca-runner.pid'
$listener = Join-Path $RunnerDirectory 'bin\Runner.Listener.exe'
function Get-OwnRunner {
    if (-not (Test-Path -LiteralPath $pidFile)) { return $null }
    $listenerPid = [int]([IO.File]::ReadAllText($pidFile).Trim())
    $candidate = Get-Process -Id $listenerPid -ErrorAction SilentlyContinue
    if ($candidate -and $candidate.Path -eq $listener) { return $candidate }
    return $null
}
if ($Action -eq 'status') {
    $running = Get-OwnRunner
    if ($running) { Write-Host "[THÔNG TIN] GitHub runner đang chạy, PID $($running.Id)." }
    else { Write-Host '[THÔNG TIN] GitHub runner đã dừng.' }
    return
}
if ($Action -eq 'stop') {
    $running = Get-OwnRunner
    if ($running) {
        # The workflow concurrency is managed by GitHub; do not interrupt an active job.
        if (Get-Process -Name 'Runner.Worker' -ErrorAction SilentlyContinue) { throw 'Runner đang xử lý job; chờ hoàn tất trước khi stop.' }
        Stop-Process -Id $running.Id
        Write-Host '[THÔNG TIN] Đã dừng GitHub runner; dữ liệu và cấu hình vẫn được giữ.'
    }
    return
}
if ($Action -eq 'setup' -and -not (Test-Path -LiteralPath (Join-Path $RunnerDirectory '.runner'))) {
    New-Item -ItemType Directory -Path $RunnerDirectory -Force | Out-Null
    Write-Host '[THÔNG TIN] Đang tải GitHub Actions runner chính thức cho Windows x64...'
    $headers = @{Accept='application/vnd.github+json';'X-GitHub-Api-Version'='2022-11-28'}
    $release = Invoke-RestMethod -Uri 'https://api.github.com/repos/actions/runner/releases/latest' -Headers $headers
    $asset = @($release.assets | Where-Object { $_.name -match '^actions-runner-win-x64-[0-9.]+\.zip$' })[0]
    if (-not $asset -or $asset.browser_download_url -notlike 'https://github.com/actions/runner/releases/download/*') { throw 'Không xác định được bản cài runner chính thức.' }
    $archive = Join-Path $RunnerDirectory $asset.name
    Invoke-WebRequest -UseBasicParsing -Uri $asset.browser_download_url -OutFile $archive
    if (-not $asset.digest -or $asset.digest -notlike 'sha256:*') { throw 'GitHub chưa cung cấp SHA-256; chưa giải nén runner.' }
    if ((Get-FileHash -LiteralPath $archive -Algorithm SHA256).Hash.ToLowerInvariant() -ne $asset.digest.Substring(7)) { throw 'Checksum runner không khớp.' }
    Expand-Archive -LiteralPath $archive -DestinationPath $RunnerDirectory -Force
    $registrationToken = $null
    try {
        if ($UseGitCredentialManager) {
            $env:GCM_INTERACTIVE = 'Never'
            $credentialText = @("protocol=https`nhost=github.com`n`n" | & git credential fill 2>$null) -join "`n"
            if ($LASTEXITCODE -ne 0 -or $credentialText -notmatch '(?m)^password=(.+)$') { throw 'Chưa có thông tin đăng nhập GitHub trong Git Credential Manager.' }
            $headers.Authorization = 'Bearer ' + $Matches[1].Trim()
            $credentialText = $null
            $response = Invoke-RestMethod -Method Post -Uri "https://api.github.com/repos/$Repository/actions/runners/registration-token" -Headers $headers
            $registrationToken = $response.token
        } else {
            $secureToken = Read-Host 'Nhập registration token từ GitHub Settings > Actions > Runners (không hiển thị)' -AsSecureString
            $tokenPointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secureToken)
            try { $registrationToken = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($tokenPointer) }
            finally { [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($tokenPointer) }
        }
        Write-Host '[THÔNG TIN] Đang đăng ký runner dưới tài khoản Windows đang sở hữu Ubuntu/WSL...'
        Push-Location -LiteralPath $RunnerDirectory
        try {
            & (Join-Path $RunnerDirectory 'config.cmd') --unattended --url "https://github.com/$Repository" --token $registrationToken --name ("plca-local-" + $env:COMPUTERNAME) --labels 'plca-local' --work '_work'
            if ($LASTEXITCODE -ne 0) { throw 'Đăng ký GitHub runner thất bại.' }
        } finally { Pop-Location }
    } finally {
        $registrationToken = $null
        $response = $null
        $headers.Remove('Authorization')
        Remove-Item Env:GCM_INTERACTIVE -ErrorAction SilentlyContinue
    }
}
if (-not (Test-Path -LiteralPath $listener) -or -not (Test-Path -LiteralPath (Join-Path $RunnerDirectory '.runner'))) { throw 'Chưa setup runner; chạy script với -Action setup.' }
$running = Get-OwnRunner
if (-not $running) {
    $process = Start-Process -FilePath $listener -ArgumentList 'run' -WorkingDirectory $RunnerDirectory -WindowStyle Hidden -RedirectStandardOutput (Join-Path $RunnerDirectory 'plca-listener.log') -RedirectStandardError (Join-Path $RunnerDirectory 'plca-listener-error.log') -PassThru
    [IO.File]::WriteAllText($pidFile, [string]$process.Id)
    Write-Host "[THÔNG TIN] Đã khởi động GitHub runner, PID $($process.Id), nhãn plca-local."
} else { Write-Host "[THÔNG TIN] GitHub runner đã chạy, PID $($running.Id)." }
