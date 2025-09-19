param(
  [string]$ComposeFile = "$(Split-Path $PSCommandPath)\docker-compose.yaml"
)

$ErrorActionPreference = "Stop"

# Generate unique identifiers per reset to avoid topic/query collisions across runs
$serviceSuffix = ([Guid]::NewGuid().ToString('N').Substring(0,8)).ToLowerInvariant()
$env:KSQL_SERVICE_ID = "ksql_service_$serviceSuffix"
$prefixStamp = [DateTime]::UtcNow.ToString('yyyyMMddHHmmssfff')
$env:KSQL_PERSISTENT_PREFIX = "phys_${serviceSuffix}_${prefixStamp}_"

[Environment]::SetEnvironmentVariable('KSQL_SERVICE_ID', $env:KSQL_SERVICE_ID)
[Environment]::SetEnvironmentVariable('KSQL_PERSISTENT_PREFIX', $env:KSQL_PERSISTENT_PREFIX)

Write-Host "[reset] using compose: $ComposeFile (serviceId=$env:KSQL_SERVICE_ID, prefix=$env:KSQL_PERSISTENT_PREFIX)"

# 1) Down with volumes to clear broker/schema data
docker compose -f $ComposeFile down -v

# 2) Clear local RocksDB / stream state left by tests (Windows + Linux paths)
try {
  if ($env:TEMP) {
    Get-ChildItem -Path "$env:TEMP" -Filter "ksql-dsl-app-*" -Directory -ErrorAction SilentlyContinue |
      ForEach-Object { Remove-Item -Recurse -Force -LiteralPath $_.FullName -ErrorAction SilentlyContinue }
  }
  if (Test-Path "/tmp") {
    Get-ChildItem -Path "/tmp" -Filter "ksql-dsl-app-*" -Directory -ErrorAction SilentlyContinue |
      ForEach-Object { Remove-Item -Recurse -Force -LiteralPath $_.FullName -ErrorAction SilentlyContinue }
  }
} catch { Write-Warning "RocksDB cleanup skipped: $($_.Exception.Message)" }

# 3) Up and wait (delegate to up.ps1 which includes health waits)
& "$(Split-Path $PSCommandPath)\up.ps1" -ComposeFile $ComposeFile

Write-Host "[reset] environment ready"
