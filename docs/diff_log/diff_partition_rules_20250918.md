# diff_partition_rules_20250918

対象: KsqlCreateStatementBuilder の PARTITION BY 判定／別名正規化ルール適用

## 変更概要
- R0/R1: GROUP BY・ウィンドウ・EMIT FINAL を含むクエリでは `PARTITION BY` を自動生成しないよう統一。
- R2: 単一 Stream ソースで明示的な再キー指定があり、既存キーと不一致な場合だけ `PARTITION BY` を挿入。キー情報が取得できない場合も安全側として挿入。
- R3: `PARTITION BY` 列を非修飾化してから安定ソートで重複除去し、曖昧性があるときだけソース名を付与。
- R4: SELECT/GROUP BY/PARTITION BY/HAVING を非修飾化し、競合する列のみ最短スコープ（`source.COLUMN`）を適用。
- CTAS/CSAS の queryId は DDL 応答ではなく `SHOW QUERIES;` 結果から照会し、対象トピック・クエリ文で突き合わせて取得。
- `SHOW QUERIES;` のレスポンスと、手動作成した内部トピックの情報を運用ログへ出力してトラブルシュート性を向上。

## 変更ファイル
- src/Query/Builders/KsqlCreateStatementBuilder.cs
  - 自動 `PARTITION BY` 条件判定を再実装し、キー比較・単一 Stream 判定・再キー指定有無を考慮。
  - 別名除去処理をメタデータベースで再構築し、曖昧列にのみスコープを残すように変更。
  - `PARTITION BY` 列の非修飾化・重複除去を安定ソートで行うユーティリティを追加。

## 動作・互換性
- 既存クエリで既存キーと同一列を指定していた場合、余分な `PARTITION BY` が出力されなくなる。
- 別名除去後も曖昧性が残るケースでは `source.COLUMN` 形式で決定論的に表現。
- 継続的クエリのスキーマ互換性に影響を与える変更は無し。

## テスト
- `dotnet build Kafka.Ksql.Linq.sln`
- `dotnet test tests/Kafka.Ksql.Linq.Tests.csproj --no-build --filter FullyQualifiedName~KsqlCreateStatementBuilderPartitionRulesTests`
