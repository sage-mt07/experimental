# diff: physical tests stabilization (2025-09-16)

目的: ksqlDB + Kafka Streams 物理テストの安定化。

変更概要:
- physicalTests/docker-compose.yaml
  - 誤記の環境変数 `KSQL_STREAMS_NUM_STREAM_THREADS` を `KSQL_KSQL_STREAMS_NUM_STREAM_THREADS` に修正。
  - 待機安定化向け設定を追加: 
    - `KSQL_KSQL_STREAMS_COMMIT_INTERVAL_MS=1000`
    - `KSQL_KSQL_STREAMS_REPLICATION_FACTOR=1`
    - `KSQL_KSQL_INTERNAL_TOPIC_REPLICAS=1`
  - 既存のヘルスチェックは `/healthcheck` を継続利用（interval=3s, retries=20, start_period=60s）。

- physicalTests/Runners/runner-entrypoint.sh
  - フェーズ化した待機ロジックを導入：
    - Phase1: SR/ksqlDB ヘルス（連続5回）
    - Phase2: `SHOW QUERIES` の応答（配列確認・エラー/exception/pending 無、連続5回）
    - Phase3: セトリング 45 秒
    - Phase4: 軽いウォームアップ（任意、失敗は無視）

- physicalTests/up.ps1
  - ローカル起動用スクリプトにも同等の待機フェーズを導入：
    - Phase1: Kafka(39092)/Schema Registry/ksqlDB health（連続5回）
    - Phase2: `SHOW QUERIES` 連続5回
    - Phase3: セトリング 45 秒

期待効果:
- 起動直後の不安定時間帯でのテスト着手を回避し、`PENDING_SHUTDOWN` 等に起因する揺らぎを低減。

注意:
- CI での所要時間は若干増加しますが、失敗再試行より総時間は短縮される想定です。
