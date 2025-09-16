param(
  [string]$ComposeFile = "$(Split-Path $PSCommandPath)\docker-compose.yaml"
)

$ErrorActionPreference = "Stop"
Write-Host "[up] using compose: $ComposeFile"

docker compose -f $ComposeFile up -d

# 簡易ヘルスチェック待機（Kafka / Schema Registry / ksqlDB）
function Wait-Http($url, $timeoutSec = 60) {
  $deadline = [DateTime]::UtcNow.AddSeconds($timeoutSec)
  while([DateTime]::UtcNow -lt $deadline){
    try {
      $resp = Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 5
      if ($resp.StatusCode -ge 200 -and $resp.StatusCode -lt 500) { return }
    } catch {}
    Start-Sleep -Seconds 2
  }
  Write-Warning "Timeout waiting for $url"
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

function Wait-Tcp($hostname, $port, $timeoutSec = 60){
  $deadline = [DateTime]::UtcNow.AddSeconds($timeoutSec)
  while([DateTime]::UtcNow -lt $deadline){
    if (Test-Tcp $hostname $port){ return }
    Start-Sleep -Seconds 2
  }
  Write-Warning "Timeout waiting TCP $($hostname):$port"
}

Write-Host "[up] waiting for Kafka(39092), SchemaRegistry(8081), ksqlDB(8088)"
Wait-Tcp localhost 39092 120
Wait-Http "http://127.0.0.1:18081/subjects" 120
Wait-Http "http://127.0.0.1:18088/healthcheck" 120
