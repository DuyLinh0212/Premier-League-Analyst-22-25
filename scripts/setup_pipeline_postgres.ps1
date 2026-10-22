param([string]$AdminUser = 'postgres', [string]$PostgresHome = 'D:\PostgreSQL\18')
$ErrorActionPreference = 'Stop'
$OutputEncoding = [System.Text.UTF8Encoding]::new($false)
try { [Console]::OutputEncoding = $OutputEncoding } catch { } # ISE/embedded hosts can have no console handle.
$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
if (-not ([Security.Principal.WindowsPrincipal]$identity).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw 'Mở PowerShell bằng Run as administrator để khởi động PostgreSQL và cấu hình kết nối WSL.'
}
$psql = Join-Path $PostgresHome 'bin\psql.exe'
if (-not (Test-Path -LiteralPath $psql)) { throw "Không tìm thấy psql: $psql" }
$project = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$linuxProject = (@(& wsl.exe -d Ubuntu --exec wslpath -u $project.Replace('\','/')) -join '').Trim()
if ($LASTEXITCODE -ne 0 -or -not $linuxProject) { throw 'Không xác định được đường dẫn dự án trong WSL.' }
$service = Get-Service 'postgresql-x64-18'
& (Join-Path $PostgresHome 'bin\pg_isready.exe') -h 127.0.0.1 -p 5432 -q
if ($LASTEXITCODE -ne 0 -and $service.Status -ne 'Running') {
    try { Start-Service -Name $service.Name }
    catch {
        throw 'Không khởi động được dịch vụ PostgreSQL. Kiểm tra log tại D:\PostgreSQL\18\data\log; không xóa hoặc tạo lại thư mục data.'
    }
} else { Write-Host '[THÔNG TIN] PostgreSQL đã nhận kết nối tại localhost:5432.' }
$secret = Read-Host 'Nhập mật khẩu PostgreSQL của tài khoản quản trị (không hiển thị)' -AsSecureString
$pointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secret)
try {
    $adminPassword = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($pointer)
    $env:PGPASSWORD = $adminPassword
    $env:PGCLIENTENCODING = 'UTF8'
    'SELECT 1;' | & $psql -h 127.0.0.1 -p 5432 -U $AdminUser -d postgres -X -v ON_ERROR_STOP=1 -q -t
    if ($LASTEXITCODE -ne 0) { throw 'Sai tài khoản/mật khẩu hoặc PostgreSQL chưa sẵn sàng.' }
    Write-Host '[THÔNG TIN] Đang đọc cấu hình PostgreSQL trong WSL...'
    $existing = @(& wsl.exe -d Ubuntu --cd $linuxProject --exec python3 -m src.pipeline.postgres_setup read) -join ''
    if ($LASTEXITCODE -ne 0) { throw 'Không đọc được cấu hình PostgreSQL trong WSL.' }
    $credentials = $existing | ConvertFrom-Json
    if (-not $credentials.writer) {
        function New-LocalPassword {
            $bytes = New-Object byte[] 32
            $random = [Security.Cryptography.RandomNumberGenerator]::Create()
            try { $random.GetBytes($bytes) } finally { $random.Dispose() }
            return [Convert]::ToBase64String($bytes).Replace('+', '-').Replace('/', '_').TrimEnd('=')
        }
        $credentials = [ordered]@{port=5432; writer=@{user='plca_writer';password=(New-LocalPassword)};
            reader=@{user='plca_reader';password=(New-LocalPassword)}; airflow=@{user='plca_airflow';password=(New-LocalPassword)}}
    }
    # Persist generated application passwords before SQL so a failed setup can resume.
    $json = $credentials | ConvertTo-Json -Depth 5 -Compress
    $json | & wsl.exe -d Ubuntu --cd $linuxProject --exec python3 -m src.pipeline.postgres_setup write
    if ($LASTEXITCODE -ne 0) { throw 'Không lưu được cấu hình tài khoản vào filesystem Linux của WSL.' }
    Write-Host '[THÔNG TIN] Đang tạo database và tài khoản riêng cho pipeline...'
    $sqlLines = @()
    foreach ($role in @('writer','reader','airflow')) {
        $item = $credentials.$role
        if ($item.user -notmatch '^plca_[a-z]+$' -or $item.password -notmatch '^[A-Za-z0-9_-]+$') { throw 'Định dạng thông tin đăng nhập cục bộ không hợp lệ.' }
        $sqlLines += "SELECT 'CREATE ROLE $($item.user) LOGIN PASSWORD ''$($item.password)''' WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname='$($item.user)')"
        $sqlLines += '\gexec'
    }
    $sqlLines += "SELECT 'CREATE DATABASE premier_league_airflow OWNER plca_airflow' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname='premier_league_airflow')"
    $sqlLines += '\gexec'
    $sqlLines += "SELECT 'CREATE DATABASE premier_league_analytics OWNER plca_writer' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname='premier_league_analytics')"
    $sqlLines += '\gexec'
    $sqlLines += 'GRANT CONNECT ON DATABASE premier_league_analytics TO plca_reader;'
    $sqlLines -join "`n" | & $psql -h 127.0.0.1 -U $AdminUser -d postgres -X -v ON_ERROR_STOP=1 -q
    if ($LASTEXITCODE -ne 0) { throw 'Không tạo được database/tài khoản dự án.' }
    # Manage only this project's entries, leaving all existing pg_hba rules intact.
    $network = (@(& wsl.exe -d Ubuntu --cd $linuxProject --exec python3 -m src.pipeline.postgres_setup network) -join '').Trim()
    if ($LASTEXITCODE -ne 0 -or $network -notmatch '^\d+\.\d+\.\d+\.\d+/\d+$') { throw 'Không dò được subnet WSL.' }
    $hba = Join-Path $PostgresHome 'data\pg_hba.conf'
    $oldText = [IO.File]::ReadAllText($hba)
    $newText = [regex]::Replace($oldText, '(?ms)^# BEGIN PLCA.*?^# END PLCA\r?\n?', '')
    $managed = "# BEGIN PLCA`r`nhost premier_league_analytics plca_writer,plca_reader $network scram-sha-256`r`nhost premier_league_airflow plca_airflow $network scram-sha-256`r`n# END PLCA`r`n"
    if (-not (Test-Path -LiteralPath ($hba + '.before-plca'))) { [IO.File]::WriteAllText($hba + '.before-plca', $oldText) }
    [IO.File]::WriteAllText($hba, $managed + $newText, [Text.UTF8Encoding]::new($false))
    $rule = Get-NetFirewallRule -Name 'PremierLeague-Postgres-WSL' -ErrorAction SilentlyContinue
    if ($rule) { $rule | Get-NetFirewallAddressFilter | Set-NetFirewallAddressFilter -RemoteAddress $network }
    else { New-NetFirewallRule -Name 'PremierLeague-Postgres-WSL' -DisplayName 'Premier League PostgreSQL from WSL' -Direction Inbound -Action Allow -Protocol TCP -LocalPort 5432 -RemoteAddress $network | Out-Null }
    'SELECT pg_reload_conf();' | & $psql -h 127.0.0.1 -U $AdminUser -d postgres -X -v ON_ERROR_STOP=1 -q -t
    if ($LASTEXITCODE -ne 0) { throw 'Không tải lại được cấu hình PostgreSQL.' }
    Write-Host '[THÔNG TIN] Đã cấu hình PostgreSQL cho Airflow và dữ liệu phân tích; mật khẩu không nằm trong Git.'
} finally {
    [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($pointer)
    Remove-Item Env:PGPASSWORD -ErrorAction SilentlyContinue
    $adminPassword = $null
}
