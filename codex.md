# Codex Tasks

## Startup Checklist (必読チェック)
- Read `AGENTS.md` and `overview.md` at session start.
- Ensure AGENTS.md and overview.md remain consistent; if one changes, review the other.
- Follow AGENTS憲章と運用ルール（diff_log/、features/ 構造、進捗エスカレーション等）。

## Physical Test Flow
1. Run docker-compose in `physicalTests/`
2. Execute dotnet physical tests
3. Collect results in `reports/physical`
4. Shutdown docker-compose
5. On success, commit reports/physical
