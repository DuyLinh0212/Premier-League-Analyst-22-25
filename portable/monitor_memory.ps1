param([int]$Seconds = 1200, [string]$Project = 'football-v3', [string]$OutputName = 'memory.json')
$taskSamples = @()
$taskDeadline = (Get-Date).AddSeconds($Seconds)
$taskOutput = Join-Path $PSScriptRoot ('evidence/' + $OutputName)
while ((Get-Date) -lt $taskDeadline) {
    $taskTotal = 0.0
    $taskContainers = @()
    foreach ($taskLine in (& docker stats --no-stream --format '{{json .}}')) {
        $taskRow = $taskLine | ConvertFrom-Json
        if (-not $taskRow.Name.StartsWith($Project + '-')) { continue }
        $taskValue = ($taskRow.MemUsage -split '/')[0].Trim()
        if ($taskValue -match '^([0-9.]+)([KMG]iB|B)$') {
            $taskFactor = switch ($Matches[2]) { 'GiB' { 1073741824 } 'MiB' { 1048576 } 'KiB' { 1024 } 'B' { 1 } }
            $taskBytes = [double]$Matches[1] * $taskFactor
            $taskTotal += $taskBytes
            $taskContainers += @{name=$taskRow.Name; bytes=$taskBytes}
        }
    }
    $taskSamples += @{utc=(Get-Date).ToUniversalTime().ToString('o'); total_bytes=$taskTotal; containers=$taskContainers}
    $taskPeak = ($taskSamples | ForEach-Object { $_.total_bytes } | Measure-Object -Maximum).Maximum
    @{project=$Project; docker_budget_bytes=6442450944; peak_container_bytes=$taskPeak; samples=$taskSamples} | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $taskOutput -Encoding utf8
    Start-Sleep -Seconds 10
}
Write-Host "Peak container RAM: $([Math]::Round($taskPeak / 1GB,3)) GiB"
