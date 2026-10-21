<#
.SYNOPSIS
    Phase 2A：进程级故障注入（kill -9 worker / orchestrator）。

.DESCRIPTION
    每轮：建任务 -> 等到有进度 -> 按端口找到 PID 并强杀 -> 用 start-service.bat 重启 ->
    等任务终态 -> 校验 checkpoint 计数与目标表行数。结果写 e2e-logs/phase2-fault-result.txt。

.EXAMPLE
    pwsh -File test-scripts/phase2-fault-injection.ps1 -DataSourceId 10 -Target worker -Rounds 3
#>
param(
    [Parameter(Mandatory = $true)][long]$DataSourceId,
    [string]$SourceTable = 'p2_load_100k',
    [ValidateSet('worker', 'orchestrator')][string]$Target = 'worker',
    [int]$Rounds = 3,
    [int]$MaxWaitMinutes = 30,
    [string]$Repo = (Split-Path -Parent $PSScriptRoot)
)

$ErrorActionPreference = 'Stop'
Set-Location $Repo
$outFile = Join-Path $Repo 'e2e-logs\phase2-fault-result.txt'
"=== Phase 2A fault injection target=$Target rounds=$Rounds @ $(Get-Date -Format o) ===" | Out-File $outFile -Encoding utf8

function Port-Up([int]$port) {
    try { return [bool](Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction Stop) } catch { return $false }
}

function Get-PidByPort([int]$port) {
    $conn = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($null -eq $conn) { return $null }
    return $conn.OwningProcess
}

function Start-Service([string]$name) {
    $bat = Join-Path $Repo "$name\start-service.bat"
    Start-Process -FilePath 'cmd.exe' -ArgumentList '/c', $bat -WorkingDirectory (Join-Path $Repo $name) -WindowStyle Hidden
}

function Wait-Port([int]$port, [int]$seconds) {
    $deadline = (Get-Date).AddSeconds($seconds)
    while ((Get-Date) -lt $deadline) {
        if (Port-Up $port) { return $true }
        Start-Sleep -Seconds 2
    }
    return $false
}

function New-Task {
    $body = @{ dataSourceId = $DataSourceId; sourceTableName = $SourceTable } | ConvertTo-Json
    (Invoke-RestMethod 'http://localhost:8082/api/tasks' -Method Post -ContentType 'application/json' -Body $body -TimeoutSec 30).taskId
}

function Get-Progress([string]$taskId) {
    try { return Invoke-RestMethod "http://localhost:8082/api/tasks/$taskId/progress" -TimeoutSec 10 } catch { return $null }
}

$results = @()
for ($i = 1; $i -le $Rounds; $i++) {
    $taskId = New-Task
    $short = $taskId.Substring(0, 8)
    $targetTable = "${SourceTable}_$short"
    Write-Output ("round {0}: task={1} target={2}" -f $i, $taskId, $targetTable)

    # 等到 worker 真正开始跑（progress 有 RUNNING 分片或 rowsIn > 0）
    $start = Get-Date
    $started = $false
    while (((Get-Date) - $start).TotalMinutes -lt 10) {
        $p = Get-Progress $taskId
        if ($null -ne $p -and (($p.rowsIn -gt 0) -or ($p.running -gt 0) -or ($p.status -in @('RUNNING', 'DISPATCHED')))) {
            $started = $true
            break
        }
        if ($null -ne $p -and $p.status -in @('SUCCEEDED', 'FAILED', 'PARTIAL_SUCCESS', 'NEEDS_ATTENTION', 'CANCELLED')) {
            break
        }
        Start-Sleep -Seconds 2
    }
    if (-not $started) { Write-Output "round $i: task finished before kill, skip" ; continue }

    $port = if ($Target -eq 'worker') { 8087 } else { 8082 }
    $pidToKill = Get-PidByPort $port
    if ($null -eq $pidToKill) { throw "no process listening on $port" }
    Write-Output ("round {0}: kill -9 pid={1} port={2}" -f $i, $pidToKill, $port)
    Stop-Process -Id $pidToKill -Force
    Start-Sleep -Seconds 2

    $svc = if ($Target -eq 'worker') { 'etl-worker' } else { 'task-management-service' }
    Start-Service $svc
    if (-not (Wait-Port $port 90)) { throw "$svc did not come back on $port" }

    # 等终态
    $final = $null
    $deadline = (Get-Date).AddMinutes($MaxWaitMinutes)
    while ((Get-Date) -lt $deadline) {
        $p = Get-Progress $taskId
        if ($null -ne $p -and $p.status -in @('SUCCEEDED', 'FAILED', 'PARTIAL_SUCCESS', 'NEEDS_ATTENTION', 'CANCELLED')) {
            $final = $p
            break
        }
        Start-Sleep -Seconds 5
    }

    $status = if ($null -ne $final) { $final.status } else { 'TIMEOUT' }
    $ck = docker exec postgres psql -U postgres -d etl_system -t -A -F ',' -c "SELECT COALESCE(SUM(rows_in),0),COALESCE(SUM(rows_out),0),COALESCE(SUM(rows_failed),0),COALESCE(SUM(rows_deduped),0) FROM etl_task_checkpoint WHERE task_id='$taskId';"
    $targetRows = docker exec postgres psql -U postgres -d etl_system -t -A -c "SELECT count(*) FROM $targetTable;"

    $row = [pscustomobject]@{
        round       = $i
        target      = $Target
        taskId      = $taskId
        status      = $status
        checkpoint  = ($ck | Out-String).Trim()
        targetRows  = ($targetRows | Out-String).Trim()
        finishedAt  = (Get-Date).ToString('o')
    }
    $results += $row
    ($row | ConvertTo-Json -Compress) | Out-File $outFile -Append -Encoding utf8
    Write-Output ($row | Format-List | Out-String)
}

Write-Output '---SUMMARY---'
Get-Content $outFile
