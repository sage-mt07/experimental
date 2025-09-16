param(
  [string]$ComposeFile = "$(Split-Path $PSCommandPath)\docker-compose.yaml"
)

$ErrorActionPreference = "Stop"
Write-Host "[up] using compose: $ComposeFile"

docker compose -f $ComposeFile up -d

function Test-Http($url){
  try {
    $resp = Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 5
    return ($resp.StatusCode -ge 200 -and $resp.StatusCode -lt 500)
  } catch { return $false }
}

function Test-Tcp($hostname, $port){
  try {
    $client = New-Object System.Net.Sockets.TcpClient
    $iar = $client.BeginConnect($hostname, $port, $null, $null)
    $ok = $iar.AsyncWaitHandle.WaitOne(3000)
    $client.Close()
    return $ok
  } catch { return $false }
}

function Test-KsqlQueriesOk(){
  $body = '{"ksql":"SHOW QUERIES;","streamsProperties":{}}'
  try {
    $resp = Invoke-WebRequest -Uri "http://127.0.0.1:18088/ksql" -UseBasicParsing -TimeoutSec 5 -Method Post -ContentType 'application/vnd.ksql+json' -Body $body
    $c = $resp.Content
    if ([string]::IsNullOrWhiteSpace($c)) { return $false }
    if ($c -match '(?i)error|exception|pending') { return $false }
    $t = $c.TrimStart()
    return $t.StartsWith('[')
  } catch { return $false }
}

# Phase 1: インフラ準備（連続5回）
Write-Host "[up] Phase 1: Infrastructure readiness (Kafka/SR/ksql health)"
$consec = 0
for($i=0; $i -lt 150; $i++){
  $ok = (Test-Tcp 'localhost' 39092) -and (Test-Http 'http://127.0.0.1:18081/subjects') -and (Test-Http 'http://127.0.0.1:18088/healthcheck')
  if ($ok) { $consec++ } else { $consec = 0 }
  if ($consec -ge 5) { break }
  Start-Sleep -Seconds 2
}

# Phase 2: ksqlDB内部状態（SHOW QUERIES 連続5回）
Write-Host "[up] Phase 2: ksqlDB internal state (SHOW QUERIES)"
$consec = 0
for($i=0; $i -lt 60; $i++){
  if (Test-KsqlQueriesOk) { $consec++ } else { $consec = 0 }
  if ($consec -ge 5) { break }
  Start-Sleep -Seconds 2
}

# Phase 3: セトリング（45秒）
Write-Host "[up] Phase 3: Settling 45s"
Start-Sleep -Seconds 45

Write-Host "[up] environment is ready"
