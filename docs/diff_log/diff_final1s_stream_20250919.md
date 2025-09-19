# diff_final1s_stream_20250919

対象: 1秒ハブ STREAM (`*_1s_final_s`) の DDL 生成フローを CSAS 方式から静的 CREATE STREAM 方式へ切り替え

## 変更概要
- 1s TABLE (`*_1s_final`) からのチェンジログを読む STREAM は、`CREATE STREAM ... WITH (...)` で既存トピックへ直接バインドするよう統一。
- WITH 句の `KAFKA_TOPIC` は 1s TABLE のトピック名そのまま（小文字）を出力し、Schema Registry のサブジェクト名と齟齬が出ないようにする。
- カラム定義は従来どおり DDL に埋め込みつつ、`AS SELECT` を伴わないシンプルな CREATE STREAM に切り替え。
- WITH 句に `KAFKA_TOPIC`/`KEY_FORMAT`/`VALUE_FORMAT`/`VALUE_AVRO_SCHEMA_FULL_NAME` に加えて `PARTITIONS`/`REPLICAS`/`RETENTION_MS` を明示出力。
- `RETENTION_MS` は 604800000 (7 日) を既定値とし、`AdditionalSettings` に `retentionMs` または `retention.ms` がある場合はそれを優先する。
- SchemaRegistry 登録結果から得られる value スキーマ名を STREAM DDL に確実に反映するため、`MappingRegistry.RegisterMeta` の戻り値を利用。
- ガイド文書を更新し、`*_1s_final_s` では列定義や `AS SELECT` を伴わないこと、RETENTION の扱いを明文化。

## 変更ファイル
- src/Query/Analysis/DerivedTumblingPipeline.cs
  - Final1sStream ブランチで WITH 句を組み立てる実装を刷新し、CSAS 生成を廃止。
  - 既定保持期間と設定上書きを処理するヘルパー (`ResolveRetentionMs`) を追加。
  - スキーマメタ登録から取得した value スキーマ名をモデルへ反映。
- tests/Query/Analysis/DerivedTumblingPipelineTests.cs
  - 新しい DDL 文字列を検証するテストを更新し、`AS SELECT` や `PARTITION BY` が出力されないことを保証。
- docs/chart-ddl.md
  - `*_1s_final_s` の作成手順を CREATE STREAM WITH 方式へ差し替え、RETENTION 取り扱いを追記。

## 動作・互換性
- 既存の 1s ハブ STREAM は再作成時に同一トピックへ再バインドされるだけで、スキーマやキー構造に互換性のない変更は発生しない。
- RETENTION を個別制御したい場合はモデルの `AdditionalSettings` にミリ秒値を設定することで従来通り上書き可能。
- ライブ／上位フレームの CTAS/CSAS 生成フローには影響無し。

## テスト
- `dotnet test tests/Kafka.Ksql.Linq.Tests.csproj --filter DerivedTumblingPipelineTests`
