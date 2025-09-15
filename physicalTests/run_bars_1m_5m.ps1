param(
  [int]$DurationMinutes = 2,
  [string]$KsqlUrl = "http://127.0.0.1:18088"
)

$ErrorActionPreference = 'Stop'

function Invoke-Ksql([string]$path, [object]$body) {
  $headers = @{ 'Content-Type'='application/vnd.ksql.v1+json; charset=utf-8'; 'Accept'='application/vnd.ksql.v1+json' }
  $json = $body | ConvertTo-Json -Depth 12
  return Invoke-RestMethod -Uri ("$KsqlUrl$path") -Method Post -Headers $headers -Body $json
}

function Write-Log([string]$msg) { Write-Host "[$((Get-Date).ToString('u'))] $msg" }

# Prepare output dir
$stamp = (Get-Date -AsUTC).ToString('yyyyMMdd_HHmmssZ')
$outDir = Join-Path (Resolve-Path ..\) "Reportsx/physical/${stamp}-1m5m"
New-Item -ItemType Directory -Force -Path $outDir | Out-Null
$push1 = Join-Path $outDir 'push_bar_1m_live.ndjson'
$push5 = Join-Path $outDir 'push_bar_5m_live.ndjson'
$pull1 = Join-Path $outDir 'pull_bar_1m_live.ndjson'
$pull5 = Join-Path $outDir 'pull_bar_5m_live.ndjson'

Write-Log "output: $outDir"

# 0) Ensure base stream
Invoke-Ksql '/ksql' @{ ksql = @"
CREATE STREAM IF NOT EXISTS DEDUPRATES (
  BROKER STRING KEY,
  SYMBOL STRING,
  TS BIGINT,
  BID DECIMAL(18,4)
) WITH (KAFKA_TOPIC='deduprates', VALUE_FORMAT='AVRO');
"@ } | Out-Null

# 1) Create 1m/5m live tables (EMIT CHANGES)
Invoke-Ksql '/ksql' @{ ksql = @"
CREATE TABLE IF NOT EXISTS bar_1m_live WITH (KAFKA_TOPIC='bar_1m_live', KEY_FORMAT='JSON', VALUE_FORMAT='JSON') AS
SELECT BROKER, SYMBOL, WINDOWSTART AS WS, WINDOWEND AS WE, COUNT(*) AS CNT
FROM DEDUPRATES WINDOW TUMBLING (SIZE 1 MINUTE)
GROUP BY BROKER, SYMBOL EMIT CHANGES;

CREATE TABLE IF NOT EXISTS bar_5m_live WITH (KAFKA_TOPIC='bar_5m_live', KEY_FORMAT='JSON', VALUE_FORMAT='JSON') AS
SELECT BROKER, SYMBOL, WINDOWSTART AS WS, WINDOWEND AS WE, COUNT(*) AS CNT
FROM DEDUPRATES WINDOW TUMBLING (SIZE 5 MINUTES)
GROUP BY BROKER, SYMBOL EMIT CHANGES;
"@ } | Out-Null

$stopAt = (Get-Date).AddMinutes($DurationMinutes)
$nextInsert = Get-Date
$nextObserve = (Get-Date).AddSeconds(15)

while ((Get-Date) -lt $stopAt) {
  $now = Get-Date
  if ($now -ge $nextInsert) {
    $ts = [int64]([DateTimeOffset](Get-Date -AsUTC)).ToUnixTimeMilliseconds()
    $sql = "INSERT INTO DEDUPRATES (BROKER, SYMBOL, TS, BID) VALUES ('B','S', $ts, 100.1234);"
    Invoke-Ksql '/ksql' @{ ksql = $sql } | Out-Null
    Write-Log "insert B,S ts=$ts"
    $nextInsert = $now.AddSeconds(2)
  }

  if ($now -ge $nextObserve) {
    try {
      $resp = Invoke-Ksql '/query-stream' @{ sql = 'SELECT * FROM bar_1m_live EMIT CHANGES LIMIT 2;' } | ConvertTo-Json -Depth 12
      @{ ts=(Get-Date -AsUTC).ToString('o'); kind='push-1m'; raw=$resp } | ConvertTo-Json -Depth 12 | Add-Content -LiteralPath $push1
    } catch { Write-Warning $_ }
    try {
      $resp = Invoke-Ksql '/query-stream' @{ sql = 'SELECT * FROM bar_5m_live EMIT CHANGES LIMIT 2;' } | ConvertTo-Json -Depth 12
      @{ ts=(Get-Date -AsUTC).ToString('o'); kind='push-5m'; raw=$resp } | ConvertTo-Json -Depth 12 | Add-Content -LiteralPath $push5
    } catch { Write-Warning $_ }
    try {
      $resp = Invoke-Ksql '/query' @{ ksql = "SELECT * FROM bar_1m_live WHERE BROKER='B' AND SYMBOL='S' LIMIT 10;" } | ConvertTo-Json -Depth 12
      @{ ts=(Get-Date -AsUTC).ToString('o'); kind='pull-1m'; raw=$resp } | ConvertTo-Json -Depth 12 | Add-Content -LiteralPath $pull1
    } catch { Write-Warning $_ }
    try {
      $resp = Invoke-Ksql '/query' @{ ksql = "SELECT * FROM bar_5m_live WHERE BROKER='B' AND SYMBOL='S' LIMIT 10;" } | ConvertTo-Json -Depth 12
      @{ ts=(Get-Date -AsUTC).ToString('o'); kind='pull-5m'; raw=$resp } | ConvertTo-Json -Depth 12 | Add-Content -LiteralPath $pull5
    } catch { Write-Warning $_ }
    Write-Log "observed 1m/5m"
    $nextObserve = $now.AddSeconds(30)
  }
  Start-Sleep -Milliseconds 200
}

# quick summary
$summary = @{
  startedUtc = (Get-Item $outDir).CreationTimeUtc.ToString('u')
  endedUtc   = (Get-Date -AsUTC).ToString('u')
  files      = (Get-ChildItem $outDir | Select-Object Name,Length)
}
$summary | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath (Join-Path $outDir 'summary.json')
"startedUtc: $($summary.startedUtc)`nendedUtc: $($summary.endedUtc)" | Set-Content -LiteralPath (Join-Path $outDir 'summary.txt')
Write-Log "done $outDir"
