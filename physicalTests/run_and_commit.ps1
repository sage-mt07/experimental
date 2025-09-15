# 実行例:
#   pwsh -NoProfile -ExecutionPolicy Bypass -File .\physicalTests\run_and_commit.ps1
# 成功したら commit。失敗時は非ゼロ終了（CIでも使える）。

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot  # c:\rc\rc01
Push-Location $repoRoot

try {
  Write-Host "=== Physical tests: START ==="

  # 1) 環境リセット（down -v → ローカル状態クリア → up & wait）
  & ".\physicalTests\reset.ps1"

  # 2) テスト
  & ".\physicalTests\test.ps1" -Solution "Kafka.Ksql.Linq.sln" -Results "reports\physical"

  Write-Host "=== Physical tests: PASSED ==="

  # 3) 成果物だけステージング（履歴汚染防止）
  git add reports/physical

  # 変更がある時だけ commit（なければスキップ）
  $status = git status --porcelain
  if (![string]::IsNullOrWhiteSpace($status)) {
    $stamp = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
    git commit -m "physical: tests passed @ $stamp"
    Write-Host "Committed: physical test results ($stamp)"
  } else {
    Write-Host "No changes to commit."
  }
}
catch {
  Write-Error "Physical test FAILED: $($_.Exception.Message)"
  exit 1
}
finally {
  # 4) 後片付けは必ず実行
  try { & ".\physicalTests\down.ps1" } catch { Write-Warning "cleanup failed: $($_.Exception.Message)" }
  Pop-Location
}
