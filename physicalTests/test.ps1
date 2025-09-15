param(
  [string]$Solution = "Kafka.Ksql.Linq.sln",
  [string]$Results = "reports\physical"
)

$ErrorActionPreference = "Stop"
New-Item -ItemType Directory -Force -Path $Results | Out-Null

# Clear local RocksDB state created by table caches / Streamiz
try {
  if ($env:TEMP) {
    Get-ChildItem -Path "$env:TEMP" -Filter "ksql-dsl-app-*" -Directory -ErrorAction SilentlyContinue | ForEach-Object {
      Remove-Item -Recurse -Force -LiteralPath $_.FullName -ErrorAction SilentlyContinue
    }
  }
  # Linux temp path when running under WSL/containers
  if (Test-Path "/tmp") {
    Get-ChildItem -Path "/tmp" -Filter "ksql-dsl-app-*" -Directory -ErrorAction SilentlyContinue | ForEach-Object {
      Remove-Item -Recurse -Force -LiteralPath $_.FullName -ErrorAction SilentlyContinue
    }
  }
} catch { Write-Warning "RocksDB cleanup skipped: $($_.Exception.Message)" }

dotnet test $Solution `
  -c Release `
  --filter "Category=Integration" `
  --logger "trx;LogFileName=physical.trx" `
  --results-directory $Results

# 追加で xUnit の xml や cobertura が必要なら logger を増やす
