<#
.SYNOPSIS
    Phase 2A：30M 行 / 20 并发任务容量基线采集。

.DESCRIPTION
    1) 确认 postgres/redis/kafka 已启动；
    2) 执行 test-scripts/phase2-load-30m.sql 造数（-SkipSql 可跳过）；
    3) 创建 1 个 30M 任务 + 20 个 100k 并发任务；
    4) 轮询任务终态，采 JVM Full GC 增量、Kafka lag、PG 连接数；
    5) 结果写 e2e-logs/phase2-load-result.json 与 .txt。

.EXAMPLE
    pwsh -File test-scripts/phase2-load-run.ps1 -DataSourceId 10
#>
param(
    [Parameter(Mandatory = $true)][long]$DataSourceId,
    [switch]$SkipSql,
    [int]$PollMinutes = 180,
    [string]$Repo = (Split-Path -Parent $PSScriptRoot)
)

$ErrorActionPreference = 'Stop'
Set-Location $Repo
$logDir = Join-Path $Repo 'e2e-logs'
$resultJson = Join-Path $logDir 'phase2-load-result.json'
$resultTxt = Join-Path $logDir 'phase2-load-result.txt'
$taskFile = Join-Path $logDir 'phase2-load-tasks.txt'

if (-not $SkipSql) {
    Write-Output '=== apply phase2-load-30m.sql (30M rows, may take minutes) ==='
    Get-Content (Join-Path $Repo 'test-scripts\phase2-load-30m.sql') -Raw |
        docker exec -i postgres psql -U postgres -d etl_system -v ON_ERROR_STOP=1
}

Write-Output '=== create tasks ==='
& pwsh -File (Join-Path $Repo 'test-scripts\phase2-load-create-tasks.ps1') -DataSourceId $DataSourceId
$tasks = Get-Content $taskFile | Where-Object { $_ -match '\|' } | ForEach-Object {
    $p = $_ -split '\|'
    [pscustomobject]@{ taskId = $p[0]; kind = $p[1]; sourceTable = $p[2] }
}

function Get-GcSnapshot {
    $snap = @{}
    foreach ($port in 8082, 8087) {
        try {
            $text = (Invoke-WebRequest "http://localhost:$port/actuator/prometheus" -UseBasicParsing -TimeoutSec 10).Content
            $sum = 0.0; $cnt = 0.0
            foreach ($line in ($text -split "`n")) {
                if ($line -match '^jvm_gc_pause_seconds_sum\{.*\}\s+([0-9.eE+-]+)$') { $sum += [double]$matches[1] }
                elseif ($line -match '^jvm_gc_pause_seconds_count\{.*\}\s+([0-9.eE+-]+)$') { $cnt += [double]$matches[1] }
            }
            $snap[$port] = [pscustomobject]@{ Sum = $sum; Count = $cnt }
        } catch {
            $snap[$port] = [pscustomobject]@{ Sum = $null; Count = $null }
        }
    }
    return $snap
}

function Get-KafkaLag {
    try {
        $out = docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group etl-worker 2>$null
    } catch { return -1 }
    $total = 0
    foreach ($line in $out) {
        if ($line -match '^\S+\s+\S+\s+\d+\s+') {
            $parts = @($line -split '\s+' | Where-Object { $_ -ne '' })
            if ($parts.Count -ge 6 -and $parts[5] -match '^\d+$') { $total += [int]$parts[5] }
        }
    }
    return $total
}

function Get-PgConnections {
    $text = docker exec postgres psql -U postgres -d etl_system -t -A -c "SELECT count(*) FROM pg_stat_activity WHERE datname='etl_system';"
    $t = ($text | Out-String).Trim()
    if ($t -match '^\d+$') { return [int]$t }
    return -1
}

$gcBefore = Get-GcSnapshot
$start = Get-Date
$maxLag = 0
$maxConns = 0
$final = @{}
$deadline = $start.AddMinutes($PollMinutes)

while ((Get-Date) -lt $deadline) {
    $pending = 0
    foreach ($t in $tasks) {
        if ($final.ContainsKey($t.taskId)) { continue }
        try {
            $task = Invoke-RestMethod "http://localhost:8082/api/tasks/task-id/$($t.taskId)" -TimeoutSec 10
            if ($task.status -in @('SUCCEEDED', 'FAILED', 'CANCELLED', 'PARTIAL_SUCCESS', 'NEEDS_ATTENTION')) {
                $final[$t.taskId] = $task.status
                Write-Output ("{0:HH:mm:ss} {1} {2} -> {3}" -f (Get-Date), $t.kind, $t.taskId.Substring(0, 8), $task.status)
            } else {
                $pending++
            }
        } catch {
            $pending++
        }
    }
    $lag = Get-KafkaLag
    if ($lag -gt $maxLag) { $maxLag = $lag }
    $conns = Get-PgConnections
    if ($conns -gt $maxConns) { $maxConns = $conns }
    if ($pending -eq 0) { break }
    Start-Sleep -Seconds 10
}

$gcAfter = Get-GcSnapshot
$rows = foreach ($t in $tasks) {
    $status = if ($final.ContainsKey($t.taskId)) { $final[$t.taskId] } else { 'TIMEOUT' }
    $short = $t.taskId.Substring(0, 8)
    $target = "p2_load_target_$short"
    $targetRowsText = docker exec postgres psql -U postgres -d etl_system -t -A -c "SELECT count(*) FROM $target;" 2>$null
    $targetRows = -1
    $tt = ($targetRowsText | Out-String).Trim()
    if ($tt -match '^\d+$') { $targetRows = [int]$tt }
    [pscustomobject]@{ taskId = $t.taskId; kind = $t.kind; sourceTable = $t.sourceTable; status = $status; targetRows = $targetRows }
}

$gcPauseDelta = -1.0
$gcCountDelta = -1.0
if ($null -ne $gcBefore[8082].Sum -and $null -ne $gcAfter[8082].Sum) {
    $gcPauseDelta = [math]::Round(($gcAfter[8082].Sum + $gcAfter[8087].Sum) - ($gcBefore[8082].Sum + $gcBefore[8087].Sum), 3)
    $gcCountDelta = [math]::Round(($gcAfter[8082].Count + $gcAfter[8087].Count) - ($gcBefore[8082].Count + $gcBefore[8087].Count), 0)
}

$summary = [pscustomobject]@{
    startedAt         = $start.ToString('o')
    finishedAt        = (Get-Date).ToString('o')
    durationSeconds   = [math]::Round(((Get-Date) - $start).TotalSeconds, 1)
    dataSourceId      = $DataSourceId
    taskCount         = $tasks.Count
    maxKafkaLag       = $maxLag
    maxPgConnections  = $maxConns
    gcPauseDeltaSec   = $gcPauseDelta
    gcCountDelta      = $gcCountDelta
    tasks             = $rows
}

$summary | ConvertTo-Json -Depth 5 | Out-File $resultJson -Encoding utf8
"=== Phase 2A load result @ $($summary.finishedAt) ===" | Out-File $resultTxt -Encoding utf8
"duration=$($summary.durationSeconds)s maxKafkaLag=$maxLag maxPgConns=$maxConns gcPauseDelta=${gcPauseDelta}s gcCountDelta=$gcCountDelta" |
    Out-File $resultTxt -Append -Encoding utf8
$rows | Format-Table -AutoSize | Out-String | Out-File $resultTxt -Append -Encoding utf8

Write-Output '---SUMMARY---'
Get-Content $resultTxt
