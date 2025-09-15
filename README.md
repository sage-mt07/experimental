# <img src="LinqKsql-logo.png" alt="LinqKsql" width="100" height="100" style="vertical-align:middle;margin-right:8px;"/> &nbsp; &nbsp; Kafka.Ksql.Linq &nbsp;&nbsp;<img src="experimental.png" alt="Experimental"  height="30" style="vertical-align:middle;margin-right:8px;"/>

## 概要
- LINQ で Kafka/ksqlDB を操作する C# ライブラリ
- Avro + Schema Registry を前提とした型安全な DSL
- Streams/Tables, Pull/Push をサポート（実行モードは自動推論）
- エラー処理（DLQ）/ リトライ / コミットの運用補助

## クイックスタート
- 前提: .NET 8, Kafka, ksqlDB, Schema Registry
- インストール: dotnet add package Kafka.Ksql.Linq
```
    await using var context = new HelloKafkaContext(configuration, LoggerFactory.Create(b => b.AddConsole()));
    var message = new HelloMessage
    {
        Id = Random.Shared.Next(),
        Text = "Hello World"
    };
    await context.HelloMessages.AddAsync(message);
    await context.HelloMessages.ForEachAsync(m =>
    {
        Console.WriteLine($"Received: {m.Text}");
        return Task.CompletedTask;
    });
```


## 構成イメージ
1) DSL 全体アーキテクチャ図
``` mermaid
flowchart TB
    subgraph App["C# アプリケーション"]
        A[LINQ / DSL 呼び出し]
    end

    A --> B[DSL]
    B --> C[Query Builder]
    C --> D[KSQL Generator]
    D -->|DDL/CSAS/CTAS| E[KsqlDB]
    E -->|Read/Write| F[(Kafka Topics)]

    %% 補助コンポーネント
    subgraph Schema["Schema Management"]
        SR[(Schema Registry)]
        AV[Avro Serializer/Deserializer]
    end

    D --> SR
    SR --- AV
    AV --- F

    %% 運用・モード
    subgraph Ops["運用機能"]
        EH[DLQ / Retry / Commit]
        MODE[Streaming Mode\nPush / Pull]
    end

    E ---> EH
    E ---> MODE

    %% キャッシュ層
    subgraph Cache["ローカルキャッシュ"]
        ST[Streamiz]
        RDB[(RocksDB)]
    end
    ST --- RDB
    ST -. 状態ストア .- E
``` 

## Examples
- 目次: `docs/examples/index.md`
  - Basics(AddAsync/ForEachAsync)
  - Query Basics（LINQ→KSQL 基本）
  - Windowing（時間窓・集計｜統合）
  - Error Handling（運用・再処理）
- OnModelCreating サンプル集: `docs/onmodelcreating_samples.md`

## ドキュメント（リファレンス）
- 利用者向け
  - SQLServer→ksqlDB ガイド: `docs/sqlserver-to-kafka-guide.md`
  - API: `docs/api_reference.md`
  - Configuration: `docs/configuration_reference.md`
- 内部構造理解のためのガイド
  - Advanced: `docs/advanced_rules.md`

## ライセンス / ロードマップ
- License: [MIT License](./LICENSE)
- Docs: 一部 CC BY 4.0 を想定
- Roadmap（例）
  - 安定化と examples 追加
  - .NET 10 対応


## Acknowledgements
- 本ライブラリは「AIと人間の共創」を理念に開発されました。[Acknowledgements](./docs/acknowledgements.md)
