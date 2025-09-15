param(
  [string]$ComposeFile = "$(Split-Path $PSCommandPath)\docker-compose.yaml"
)

$ErrorActionPreference = "Stop"
Write-Host "[down] using compose: $ComposeFile"

docker compose -f $ComposeFile down -v
