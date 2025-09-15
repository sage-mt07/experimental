# Kafka.Ksql.Linq 機能一覧（利用者向け）

Kafka/KSQL を LINQ で扱う C# DSL を使いこなすための実践ガイドです。

---

## コア機能

### Entity モデルとアノテーション
`[KsqlTopic]`, `[KsqlTimestamp]`, `[KsqlTable]` でトピックやテーブルを定義し、`Entity<T>()` でモデルを登録してスキーマを再利用する。

### クエリ DSL
`ToQuery` に `From/Join/Where/Select` を連鎖し、式木を KSQL クエリへ最適化して永続ビューを構築する。

### メッセージ送受信 API
`AddAsync` と `ForEachAsync` で送信と Push/Pull コンシュームを一元化し、バックプレッシャやリトライ、コミット戦略を設定できる。

### DLQ とエラー処理
`.OnError(ErrorAction.DLQ)` で処理失敗を DLQ へ送り、`ctx.Dlq.ForEachAsync` で死んだレコードを巡回する。

---

## 利用の流れ

### コンテキストを構築して開始する
設定と Schema Registry URL を渡し、ログを有効化して `KsqlContextBuilder` でコンテキストを作成する。

```csharp
var configuration = new ConfigurationBuilder()
  .AddJsonFile("appsettings.json")
  .Build();

var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());

var ctx = KsqlContextBuilder.Create()
  .UseConfiguration(configuration)
  .UseSchemaRegistry(configuration["KsqlDsl:SchemaRegistry:Url"]!)
  .EnableLogging(loggerFactory)
  .BuildContext<MyAppContext>();
```

### エンティティを定義して登録する
`[KsqlTopic]` と `[KsqlTimestamp]` を付与し、`EventSet<T>` プロパティを公開してモデル登録する。

```csharp
[KsqlTopic("basic-produce-consume")]
public class BasicMessage
{
  public int Id { get; set; }
  [KsqlTimestamp] public DateTime CreatedAt { get; set; }
  public string Text { get; set; } = string.Empty;
}

[KsqlTable("hourly-counts")]
public class HourlyCount
{
  [KsqlKey] public int Hour { get; set; }
  public long Count { get; set; }
}

public class MyAppContext : KsqlContext
{
  // 複数エンティティを公開可能
  public EventSet<BasicMessage> BasicMessages { get; set; } = null!;
  public EventSet<AnotherEntity> AnotherEntities { get; set; } = null!;
  public EntitySet<OrderSummary> OrderSummaries { get; set; } = null!;
  public EntitySet<HourlyCount> HourlyCounts { get; set; } = null!;
}
```

このモデルに対応する KSQL DDL は次のとおり。

```sql
CREATE STREAM BASICMESSAGE (
  Id INT KEY,
  CreatedAt TIMESTAMP,
  Text VARCHAR
) WITH (
  KAFKA_TOPIC='basic-produce-consume',
  KEY_FORMAT='AVRO',
  VALUE_FORMAT='AVRO',
  VALUE_AVRO_SCHEMA_FULL_NAME='your.app.BasicMessage',
  TIMESTAMP='CreatedAt'
);

CREATE TABLE HOURLYCOUNT (
  HOUR INT PRIMARY KEY,
  COUNT BIGINT
) WITH (
  KAFKA_TOPIC='hourly-counts',
  KEY_FORMAT='AVRO',
  VALUE_FORMAT='AVRO',
  VALUE_AVRO_SCHEMA_FULL_NAME='your.app.HourlyCount'
);
```

### メッセージを送受信する
`AddAsync` で送信し、`ForEachAsync` で受信を確認する。

```csharp
// 送信
await ctx.BasicMessages.AddAsync(new BasicMessage
{
  Id = Random.Shared.Next(),
  CreatedAt = DateTime.UtcNow,
  Text = "Basic Flow"
});

// 受信
await ctx.BasicMessages.ForEachAsync(async m =>
{
  Console.WriteLine($"Consumed: {m.Text}");
  await Task.CompletedTask;
});
```

ヘッダーやメタデータを扱う例。`autoCommit: false` で手動コミットに切り替えられる。

```csharp
var headers = new Dictionary<string, string> { ["source"] = "api" };
using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

await ctx.BasicMessages.AddAsync(
  new BasicMessage { Id = 1, CreatedAt = DateTime.UtcNow, Text = "With header" },
  headers,
  cts.Token);

await ctx.BasicMessages.ForEachAsync(
  (m, h, meta) =>
  {
    Console.WriteLine($"{meta.Topic}:{meta.Offset} => {h["source"]}");
    ctx.BasicMessages.Commit(m);
    return Task.CompletedTask;
  },
  timeout: TimeSpan.FromSeconds(10),
  autoCommit: false);
```

### ビューを定義してクエリする
`ToQuery` に `From/Join/Where/Select` を連鎖させ、永続ビューを定義する。

```csharp
modelBuilder.Entity<OrderSummary>().ToQuery(q => q
  .From<Order>()
  .Join<Customer>((o, c) => o.CustomerId == c.Id)
  .Where((o, c) => c.IsActive)
  .Select((o, c) => new OrderSummary
  {
    OrderId = o.Id,
    CustomerName = c.Name
  }));

// クエリ実行
var summaries = await ctx.OrderSummaries.ToListAsync();
```

このクエリは次の KSQL に変換される。

```sql
CREATE TABLE ORDERSUMMARY AS
  SELECT
    O.ID AS ORDERID,
    C.NAME AS CUSTOMERNAME
  FROM ORDER O
  JOIN CUSTOMER C
    ON O.CUSTOMERID = C.ID
  WHERE C.ISACTIVE = TRUE;
```

ウィンドウと集計を組み合わせた例。

```csharp
modelBuilder.Entity<HourlyCount>().ToQuery(q => q
  .From<BasicMessage>()
  .Tumbling(m => m.CreatedAt, new Windows { Hours = new[] { 1 } })
  .GroupBy(m => m.CreatedAt.Hour)
  .Select(g => new HourlyCount { Hour = g.Key, Count = g.Count() }));

var counts = await ctx.HourlyCounts.ToListAsync();
```

このクエリは次の KSQL に変換される。

```sql
CREATE TABLE HOURLYCOUNT AS
  SELECT
    EXTRACT(HOUR FROM CreatedAt) AS HOUR,
    COUNT(*) AS COUNT
  FROM BASICMESSAGE
  WINDOW TUMBLING (SIZE 1 HOUR)
  GROUP BY EXTRACT(HOUR FROM CreatedAt);
```

### 失敗を DLQ で追跡する
`.OnError(ErrorAction.DLQ)` 付き `ForEachAsync` は処理中の例外を DLQ に転送し、DLQ ストリームは `ctx.Dlq.ForEachAsync` で巡回できる。

```csharp
await ctx.BasicMessages
  .OnError(ErrorAction.DLQ)
  .ForEachAsync(m => throw new Exception("fail"));

await ctx.Dlq.ForEachAsync(r =>
{
  Console.WriteLine(r.RawText);
  return Task.CompletedTask;
});
```

### テーブルを一覧取得する
`ToListAsync` でテーブル内容を読み取り、結果はローカルキャッシュから提供される。

```csharp
var counts = await ctx.HourlyCounts.ToListAsync();
foreach (var c in counts)
  Console.WriteLine($"{c.Hour}: {c.Count}");
```

## 拡張ポイント

### DI とロギングを差し替える
`IServiceCollection` にコンテキストを登録し、既存ロガーを流用する。

```csharp
var services = new ServiceCollection();
services.AddSingleton(sp =>
  KsqlContextBuilder.Create()
    .UseConfiguration(sp.GetRequiredService<IConfiguration>())
    .EnableLogging(sp.GetRequiredService<ILoggerFactory>())
    .BuildContext<MyAppContext>());
```

### バリデーションやタイムアウトを調整する
スキーマ登録や検証の挙動を `ConfigureValidation` と `WithTimeouts` で制御する。

```csharp
var ctx = KsqlContextBuilder.Create()
  .UseConfiguration(configuration)
  .ConfigureValidation(autoRegister: false, failOnErrors: true)
  .WithTimeouts(TimeSpan.FromSeconds(30))
  .BuildContext<MyAppContext>();
```

## 運用・監視
- **設定ファイル**: `BootstrapServers`・`SchemaRegistry.Url`・`KsqlDbUrl`・DLQ 設定を `appsettings.json` に集約する。
- **メトリクス/ヘルスチェック**: プロデューサやコンシューマのレイテンシとエラーカウントを `ILogger` 経由で収集する。
- **スキーマ管理**: Schema Registry の互換性モード (FORWARD/STRICT) を設定し、互換性を検証する。
- **テスト**: コンテキストを In-Memory 実装に差し替え、ユニットテストを容易にする。

---

## 代表的なアノテーションと API
- `[KsqlTopic]` / `[KsqlTimestamp]` / `[KsqlTable]`
- `Entity<T>()`, `ToQuery(...)`, `AddAsync`, `ForEachAsync`, `ToListAsync`, `ctx.Dlq.ForEachAsync`

---

## 最小構成例 (appsettings.json)
`BootstrapServers`・`SchemaRegistry.Url`・`KsqlDbUrl` を最低限記述する。

```json
{
  "KsqlDsl": {
    "Common": { "BootstrapServers": "localhost:9092", "ClientId": "app" },
    "SchemaRegistry": { "Url": "http://localhost:8085" },
    "KsqlDbUrl": "http://localhost:8088",
    "DlqTopicName": "dead-letter-queue",
    "DeserializationErrorPolicy": "DLQ"
  }
}
```

エンティティごとの Producer/Consumer/Topic 設定は `Streams` セクションで上書きできる。

```json
{
  "KsqlDsl": {
    "Streams": {
      "basic-produce-consume": {
        "Producer": { "Acks": "All" },
        "Consumer": { "GroupId": "custom-group" },
        "Creation": { "NumPartitions": 3 }
      }
    }
  }
}
```
