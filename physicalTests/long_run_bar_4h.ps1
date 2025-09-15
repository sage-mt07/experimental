param(
  [int]$DurationMinutes = 240,
  [string]$ComposeFile = "$(Split-Path $PSCommandPath)\docker-compose.yaml",
  [string]$KsqlUrl = "http://127.0.0.1:18088"
)

$ErrorActionPreference = 'Stop'

function Invoke-Ksql([string]$path, [string]$method = 'GET', [object]$body = $null) {
  $uri = "$KsqlUrl$path"
  if ($null -ne $body) {
    $json = ($body | ConvertTo-Json -Depth 8)
    return Invoke-RestMethod -Uri $uri -Method $method -ContentType 'application/json' -Body $json
  } else {
    return Invoke-RestMethod -Uri $uri -Method $method
  }
}

function Write-Log([string]$msg) {
  $ts = (Get-Date).ToString('u')
  Write-Host "[$ts] $msg"
}

# 0) Reset environment per validation doc
Write-Log "reset environment"
& "$(Split-Path $PSCommandPath)\reset.ps1" -ComposeFile $ComposeFile

# 1) Create sources (DEDUPRATES, MSCHED) — idempotent
Write-Log "create sources"
Invoke-Ksql '/ksql' 'POST' @{ ksql = @'
CREATE STREAM IF NOT EXISTS DEDUPRATES (
  BROKER STRING KEY,
  SYMBOL STRING,
  TS BIGINT,
  BID DECIMAL(18,4)
) WITH (KAFKA_TOPIC=''deduprates'', VALUE_FORMAT=''AVRO'');

CREATE TABLE IF NOT EXISTS MSCHED (
  BROKER STRING PRIMARY KEY,
  SYMBOL STRING,
  OPEN_TS BIGINT,
  CLOSE_TS BIGINT
) WITH (KAFKA_TOPIC=''msched'', VALUE_FORMAT=''AVRO'');
'@ } | Out-Null

# 2) Create CSAS views (bar_1d_live / bar_1wk_final) — simplified representatives
Write-Log "create CSAS tables"
Invoke-Ksql '/ksql' 'POST' @{ ksql = @'
CREATE TABLE IF NOT EXISTS bar_1d_live WITH (KAFKA_TOPIC=''bar_1d_live'') AS
SELECT BROKER, SYMBOL, WINDOWSTART AS WS, WINDOWEND AS WE, COUNT(*) AS CNT
FROM DEDUPRATES WINDOW TUMBLING (SIZE 1 DAY)
GROUP BY BROKER, SYMBOL EMIT CHANGES;

CREATE TABLE IF NOT EXISTS bar_1wk_final WITH (KAFKA_TOPIC=''bar_1wk_final'') AS
SELECT BROKER, SYMBOL, WINDOWSTART AS WS, WINDOWEND AS WE, COUNT(*) AS CNT
FROM DEDUPRATES WINDOW TUMBLING (SIZE 7 DAYS)
GROUP BY BROKER, SYMBOL EMIT FINAL;
'@ } | Out-Null

# 3) Ingestion loop (10–60s). Observation loop (Push/Pull) each 1–10m.
$stopAt = (Get-Date).AddMinutes($DurationMinutes)
$nextInsert = Get-Date
$nextObserve = (Get-Date).AddMinutes(1)

$utc = (Get-Date -AsUTC).ToString('yyyyMMdd_HHmmssZ')
$reportDir = Join-Path (Resolve-Path ..\) "Reportsx\physical\$utc"
New-Item -ItemType Directory -Force -Path $reportDir | Out-Null
$pushLog = Join-Path $reportDir 'push_bar_1d_live.ndjson'
$pullLog = Join-Path $reportDir 'pull_bar_1wk_final.ndjson'

Write-Log "start long-run: $DurationMinutes min; reporting to $reportDir"

while ((Get-Date) -lt $stopAt) {
  $now = Get-Date
  if ($now -ge $nextInsert) {
    $ts = [int64]([DateTimeOffset](Get-Date -AsUTC)).ToUnixTimeMilliseconds()
    $body = @{ ksqs = @() }
    $sql = "INSERT INTO DEDUPRATES (BROKER, SYMBOL, TS, BID) VALUES ('B','S', $ts, 100.1234);"
    Invoke-Ksql '/ksql' 'POST' @{ ksql = $sql } | Out-Null
    Write-Log "inserted deduprate ts=$ts"
    $nextInsert = $now.AddSeconds((Get-Random -Minimum 10 -Maximum 60))
  }

  if ($now -ge $nextObserve) {
    # Push (query-stream) limited
    try {
      $pushBody = @{ sql = "SELECT * FROM bar_1d_live EMIT CHANGES LIMIT 2;" }
      $pushResp = Invoke-Ksql '/query-stream' 'POST' $pushBody | ConvertTo-Json -Depth 12
      $wrap = @{ ts = (Get-Date).ToUniversalTime().ToString('o'); kind = 'push'; raw = $pushResp }
      Add-Content -LiteralPath $pushLog -Value ($wrap | ConvertTo-Json -Depth 12)
    } catch { Write-Warning $_ }

    # Pull (query)
    try {
      $pullBody = @{ sql = "SELECT * FROM bar_1wk_final WHERE BROKER='B' AND SYMBOL='S' LIMIT 10;" }
      $pullResp = Invoke-Ksql '/query' 'POST' $pullBody | ConvertTo-Json -Depth 12
      $wrap = @{ ts = (Get-Date).ToUniversalTime().ToString('o'); kind = 'pull'; raw = $pullResp }
      Add-Content -LiteralPath $pullLog -Value ($wrap | ConvertTo-Json -Depth 12)
    } catch { Write-Warning $_ }

    Write-Log "observed push/pull; logs appended"
    $nextObserve = $now.AddMinutes((Get-Random -Minimum 1 -Maximum 10))
  }

  Start-Sleep -Seconds 1
}

Write-Log "long-run finished; collecting SHOW TABLES"
try {
  $tables = Invoke-Ksql '/ksql' 'POST' @{ ksql = 'SHOW TABLES;' }
  $tables | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $reportDir 'show_tables.json')
} catch { Write-Warning $_ }

Write-Log "done. See $reportDir"
