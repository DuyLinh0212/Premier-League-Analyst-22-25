param([string]$RawSource = $env:PLCA_RAW_SOURCE)
$ErrorActionPreference = 'Stop'
if (-not $env:GITHUB_WORKSPACE) { throw 'Script này chỉ chuẩn bị dữ liệu trong workspace riêng của GitHub runner.' }
if (-not $RawSource) { $RawSource = 'F:\NgDuyLinh\Do_an\BigData\Premier-League-Analyst-22-25\data\raw' }
$source = (Resolve-Path -LiteralPath $RawSource).Path
$workspace = (Resolve-Path -LiteralPath $env:GITHUB_WORKSPACE).Path.TrimEnd('\')
$destination = [IO.Path]::GetFullPath((Join-Path $workspace 'data\raw'))
if (-not $destination.StartsWith($workspace + '\', [StringComparison]::OrdinalIgnoreCase)) { throw 'Đích nằm ngoài workspace CI.' }
if ($source -eq $destination) { throw 'Nguồn dữ liệu phải tách khỏi checkout CI.' }
$files = @(Get-ChildItem -LiteralPath $source -Recurse -File)
if ($files.Count -ne 586) { throw "Cần đúng 586 file nguồn, hiện có $($files.Count)." }
if (Test-Path -LiteralPath $destination) { throw 'Thư mục raw của CI đã tồn tại; dùng checkout mới, không xóa nguồn đang làm việc.' }
New-Item -ItemType Directory -Path $destination -Force | Out-Null
Get-ChildItem -LiteralPath $source | ForEach-Object { Copy-Item -LiteralPath $_.FullName -Destination $destination -Recurse }
Write-Host '[THÔNG TIN] Đã sao chép 586 file thô vào checkout CI riêng; nguồn hiện tại giữ nguyên.'
