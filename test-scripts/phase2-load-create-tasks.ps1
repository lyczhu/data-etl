<#
.SYNOPSIS
    Phase 2A：创建 30M 主任务 + 20 个并发小任务。

.DESCRIPTION
    直接调用 task-management-service 的提交接口（8082），绕开网关鉴权。
    提交即走新链路：createRun -> validate -> QUEUED -> dispatch。
    输出 taskId 清单到 e2e-logs/phase2-load-tasks.txt，供 phase2-load-run.ps1 轮询。

.EXAMPLE
    pwsh -File test-scripts/phase2-load-create-tasks.ps1 -DataSourceId 10
#>
param(
    [Parameter(Mandatory = $true)][long]$DataSourceId,
    [string]$BaseUrl = 'http://localhost:8082',
    [string]$LargeTable = 'p2_load_30m',
    [string]$SmallTable = 'p2_load_100k',
    [int]$ConcurrentTasks = 20,
    [string]$OutFile = 'e2e-logs/phase2-load-tasks.txt'
)

$ErrorActionPreference = 'Stop'

function New-Task([string]$sourceTable, [string]$kind) {
    $body = @{ dataSourceId = $DataSourceId; sourceTableName = $sourceTable } | ConvertTo-Json
    $task = Invoke-RestMethod "$BaseUrl/api/tasks" -Method Post -ContentType 'application/json' -Body $body -TimeoutSec 30
    return [pscustomobject]@{ taskId = $task.taskId; kind = $kind; sourceTable = $sourceTable }
}

$tasks = @()
$tasks += New-Task $LargeTable 'large'
for ($i = 1; $i -le $ConcurrentTasks; $i++) {
    $tasks += New-Task $SmallTable 'concurrent'
}

$dir = Split-Path -Parent $OutFile
if ($dir -and -not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir | Out-Null }
$tasks | ForEach-Object { "$($_.taskId)|$($_.kind)|$($_.sourceTable)" } | Out-File $OutFile -Encoding utf8

Write-Output ("created {0} tasks (1 large + {1} concurrent), dataSourceId={2}" -f $tasks.Count, $ConcurrentTasks, $DataSourceId)
$tasks | Format-Table -AutoSize
