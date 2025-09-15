# Kafka.Ksql.Linq Feature Guide (User Edition)

This guide helps you master the C# DSL that lets you control Kafka/ksqlDB with LINQ.

---

## Core capabilities

### Entity models and annotations
Use `[KsqlTopic]`, `[KsqlTimestamp]`, and `[KsqlTable]` to define topics and tables, then register models with `Entity<T>()` so schemas can be reused.

### Query DSL
Chain `From/Join/Where/Select` inside `ToQuery` to build persistent views. The expression tree is optimized into KSQL statements.

### Messaging API
`AddAsync` and `ForEachAsync` unify send and push/pull consumption. Configure backpressure, retries, and commit strategies.

### DLQ and error handling
`.OnError(ErrorAction.DLQ)` routes failures to the DLQ. Inspect records via `ctx.Dlq.ForEachAsync`.

---

## Usage flow

### Build the context
Provide configuration and the Schema Registry URL, enable logging, and construct the context with `KsqlContextBuilder`.

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

### Define and register entities
Annotate with `[KsqlTopic]` and `[KsqlTimestamp]`, expose `EventSet<T>` properties, and register them in the context.

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
  // expose multiple entities
  public EventSet<BasicMessage> BasicMessages { get; set; } = null!;
  public EventSet<AnotherEntity> AnotherEntities { get; set; } = null!;
  public EntitySet<OrderSummary> OrderSummaries { get; set; } = null!;
  public EntitySet<HourlyCount> HourlyCounts { get; set; } = null!;
}
```

The matching KSQL DDL looks like this:

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

### Send and receive messages
Send with `AddAsync`, verify with `ForEachAsync`.

```csharp
// Send
await ctx.BasicMessages.AddAsync(new BasicMessage
{
  Id = Random.Shared.Next(),
  CreatedAt = DateTime.UtcNow,
  Text = "Basic Flow"
});

// Receive
await ctx.BasicMessages.ForEachAsync(async m =>
{
  Console.WriteLine($"Consumed: {m.Text}");
  await Task.CompletedTask;
});
```

Handle headers and metadata; switch to manual commit with `autoCommit: false`.

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

### Define views and query them
Chain `From/Join/Where/Select` inside `ToQuery` to create persistent views.

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

// Query execution
var summaries = await ctx.OrderSummaries.ToListAsync();
```

The generated KSQL:

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

Combine windows and aggregations:

```csharp
modelBuilder.Entity<HourlyCount>().ToQuery(q => q
  .From<BasicMessage>()
  .Tumbling(m => m.CreatedAt, new Windows { Hours = new[] { 1 } })
  .GroupBy(m => m.CreatedAt.Hour)
  .Select(g => new HourlyCount { Hour = g.Key, Count = g.Count() }));

var counts = await ctx.HourlyCounts.ToListAsync();
```

KSQL output:

```sql
CREATE TABLE HOURLYCOUNT AS
  SELECT
    EXTRACT(HOUR FROM CreatedAt) AS HOUR,
    COUNT(*) AS COUNT
  FROM BASICMESSAGE
  WINDOW TUMBLING (SIZE 1 HOUR)
  GROUP BY EXTRACT(HOUR FROM CreatedAt);
```

### Track failures with the DLQ
`.OnError(ErrorAction.DLQ)` forwards failures to the DLQ; read them with `ctx.Dlq.ForEachAsync`.

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

### Enumerate tables
`ToListAsync` reads table contents from the local cache.

```csharp
var counts = await ctx.HourlyCounts.ToListAsync();
foreach (var c in counts)
  Console.WriteLine($"{c.Hour}: {c.Count}");
```

## Extensibility

### Replace DI and logging
Register the context in `IServiceCollection` and reuse existing loggers.

```csharp
var services = new ServiceCollection();
services.AddSingleton(sp =>
  KsqlContextBuilder.Create()
    .UseConfiguration(sp.GetRequiredService<IConfiguration>())
    .EnableLogging(sp.GetRequiredService<ILoggerFactory>())
    .BuildContext<MyAppContext>());
```

### Adjust validation and timeouts
Control schema registration and validation via `ConfigureValidation`; override timeouts with `WithTimeouts`.

```csharp
var ctx = KsqlContextBuilder.Create()
  .UseConfiguration(configuration)
  .ConfigureValidation(autoRegister: false, failOnErrors: true)
  .WithTimeouts(TimeSpan.FromSeconds(30))
  .BuildContext<MyAppContext>();
```

## Operations and monitoring
- **Configuration**: place `BootstrapServers`, `SchemaRegistry.Url`, `KsqlDbUrl`, and DLQ settings in `appsettings.json`.
- **Metrics/health**: capture producer/consumer latency and error counts via `ILogger`.
- **Schema governance**: set Schema Registry compatibility (FORWARD/STRICT) and verify compatibility.
- **Testing**: swap the context with an in-memory implementation for unit tests.

---

## Key annotations and APIs
- `[KsqlTopic]`, `[KsqlTimestamp]`, `[KsqlTable]`
- `Entity<T>()`, `ToQuery(...)`, `AddAsync`, `ForEachAsync`, `ToListAsync`, `ctx.Dlq.ForEachAsync`

---

## Minimal appsettings.json
Record at least `BootstrapServers`, `SchemaRegistry.Url`, and `KsqlDbUrl`.

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

Override per-entity Producer/Consumer/Topic settings under `Streams`.

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
