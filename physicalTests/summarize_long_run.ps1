param(
  [Parameter(Mandatory=$true)] [string]$ReportDir
)

$ErrorActionPreference = 'Stop'
if (-not (Test-Path $ReportDir)) { throw "ReportDir not found: $ReportDir" }

function Read-Ndjson([string]$path) {
  if (-not (Test-Path $path)) { return @() }
  Get-Content -LiteralPath $path | ForEach-Object { try { $_ | ConvertFrom-Json } catch { $null } } | Where-Object { $_ -ne $null }
}

$pushPath = Join-Path $ReportDir 'push_bar_1d_live.ndjson'
$pullPath = Join-Path $ReportDir 'pull_bar_1wk_final.ndjson'
$tablesPath = Join-Path $ReportDir 'show_tables.json'

$push = Read-Ndjson $pushPath
$pull = Read-Ndjson $pullPath

function MinMax([object[]]$items, [string]$prop) {
  if ($items.Count -eq 0) { return @{ min=$null; max=$null } }
  $vals = $items | ForEach-Object { [datetime]::Parse($_.$prop) }
  return @{ min = ($vals | Measure-Object -Minimum).Minimum; max = ($vals | Measure-Object -Maximum).Maximum }
}

$p = MinMax $push 'ts'
$q = MinMax $pull 'ts'
$start = ($p.min, $q.min | Where-Object { $_ } | Sort-Object | Select-Object -First 1)
$end   = ($p.max, $q.max | Where-Object { $_ } | Sort-Object -Descending | Select-Object -First 1)
$dur   = if ($start -and $end) { New-TimeSpan -Start $start -End $end } else { $null }

$summary = [pscustomobject]@{
  startedUtc   = if ($start) { $start.ToString('u') } else { $null }
  endedUtc     = if ($end) { $end.ToString('u') } else { $null }
  duration     = if ($dur) { [int]$dur.TotalMinutes } else { $null }
  pushSamples  = $push.Count
  pullSamples  = $pull.Count
  reportDir    = (Resolve-Path $ReportDir).Path
}

$summary | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath (Join-Path $ReportDir 'summary.json')
$summary | Format-List | Out-String | Set-Content -LiteralPath (Join-Path $ReportDir 'summary.txt')

Write-Host "Summary written:" (Join-Path $ReportDir 'summary.txt')

