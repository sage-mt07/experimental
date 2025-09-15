param(
  [string]$ComposeFile = "$(Split-Path $PSCommandPath)\docker-compose.yaml"
)

$ErrorActionPreference = "Stop"
Write-Host "[reset] using compose: $ComposeFile"

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

