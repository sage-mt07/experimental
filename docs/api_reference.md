# API Reference

This page guides you through sending, receiving, and defining views.

## Typical workflow

### 1. Build a context (start with configuration)
- Load configuration and pass it to the builder.
- Specify the Schema Registry URL.
- Enable logging to inspect generated queries.

```csharp
var configuration = new ConfigurationBuilder()
  .AddJsonFile("appsettings.json").Build();

var ctx = KsqlContextBuilder.Create()
  .UseConfiguration(configuration)
  .UseSchemaRegistry(configuration["KsqlDsl:SchemaRegistry:Url"]!)
  .EnableLogging(LoggerFactory.Create(b => b.AddConsole()))
  .BuildContext<MyAppContext>();
```

- Summary: create `ctx` first; subsequent operations start from it.

### 2. Register entities (choose the types you use)
- Use `[KsqlTopic]` to specify topic names.
- Mark timestamps with `[KsqlTimestamp]`.
- Register with `Entity<T>()` in `OnModelCreating`.

```csharp
[KsqlTopic("basic-produce-consume")]
public class BasicMessage
{
  public int Id { get; set; }
  [KsqlTimestamp] public DateTime CreatedAt { get; set; }
  public string Text { get; set; } = string.Empty;
}

protected override void OnModelCreating(IModelBuilder b)
  => b.Entity<BasicMessage>();
```

- Summary: after registration, `ctx.Set<BasicMessage>()` becomes available.

### 3. Send, receive, and verify
- Call `AddAsync` to send.
- Wait briefly and then subscribe with `ForEachAsync`.
- Confirm the expected message on standard output.

```csharp
await ctx.Set<BasicMessage>().AddAsync(new BasicMessage
{
  Id = Random.Shared.Next(),
  CreatedAt = DateTime.UtcNow,
  Text = "Basic Flow"
});

await Task.Delay(500);
await ctx.Set<BasicMessage>().ForEachAsync(m =>
{
  Console.WriteLine($"Consumed: {m.Text}");
  return Task.CompletedTask;
});
```

- Expected result: `Consumed: Basic Flow` prints.
- Summary: send and receive using two `Set<T>()` calls.

### 4. Define a view (ToQuery)
- Declare persistent views with `ToQuery(...)`.
- Chain `From/Join/Where/Select`.
- Use LINQ for temporary filters.

```csharp
modelBuilder.Entity<OrderSummary>().ToQuery(q => q
  .From<Order>()
  .Join<Customer>((o, c) => o.CustomerId == c.Id)
  .Where((o, c) => c.IsActive)
  .Select((o, c) => new OrderSummary { OrderId = o.Id, CustomerName = c.Name }));

await ctx.Set<OrderSummary>()
  .Where(x => x.CustomerName.StartsWith("A"))
  .ForEachAsync(x => { /* consume */ return Task.CompletedTask; });
```

- Summary: `ToQuery` defines the view; LINQ handles temporary filters.

### 5. Catch failures (follow the DLQ)
- Failed records go to the dead-letter queue.
- Inspect them with `ctx.Dlq.ReadAsync()`.
- Fix and resend if necessary.

```csharp
await foreach (var rec in ctx.Dlq.ReadAsync())
{
  Console.WriteLine(rec.RawText);
}
```

- Summary: scan the DLQ to find anomalies.

---

## Core annotations and APIs
- `[KsqlTopic]`: binds an entity to a topic.
- `[KsqlTimestamp]`: handles event time in Avro-compatible form.
- `[KsqlTable]`: marks an entity as a table (streams are default).
- `Entity<T>()`: registers a type for operations.
- `ToQuery(...), Where(...)`: define views and filters.
- `OnError(...), ctx.Dlq.ReadAsync()`: handle and investigate errors.

---

## Key API signatures
- `IEventSet<T> AddAsync(T entity, CancellationToken ct = default)`
  - Sends a record. Returns `ValueTask`; throws `KafkaException` on failure.
- `IEventSet<T> ForEachAsync(Func<T,Task> handler, CancellationToken ct = default)`
  - Consumes records with a handler; cancel to stop.
- `ModelBuilder.Entity<T>(bool readOnly=false, bool writeOnly=false)`
  - Registers the type. Defaults to both false (read/write).
- `QueryBuilder ToQuery(Func<IQueryBuilder,IQueryBuilder> build)`
  - Declares a view. Applies KSQL at generation.

Summary: memorize the essentials for send/receive/register/define succinctly.

## Configuration schema (minimal)

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

- Required: `Common.BootstrapServers`, `SchemaRegistry.Url`, `KsqlDbUrl`
- Recommended: `DlqTopicName`, `DeserializationErrorPolicy`

Summary: With these settings the minimal configuration works.

## Type excerpt (DLQ etc.)

```csharp
public sealed class DlqRecord
{
  public string SourceTopic { get; init; } = "";
  public string ErrorCode  { get; init; } = "";
  public string RawText    { get; init; } = "";
}
```

- Inspect `RawText` to isolate the cause.

Summary: check `RawText`, then `SourceTopic`.

## Example of generated KSQL (representative)

```csharp
modelBuilder.Entity<OrderView>().ToQuery(q => q
  .From<Order>()
  .Where(o => o.Amount > 0)
  .Select(o => new OrderView { Id = o.Id, Amount = o.Amount }));
```

Conceptual output:
```
CREATE STREAM OrderView AS
SELECT Id, Amount
FROM Order
WHERE Amount > 0;
```

Summary: `ToQuery` produces CSAS/CTAS-style KSQL.

---

## API Reference (catalog)

### Attributes
- `[KsqlTopic(name)]`: binds an entity to a Kafka topic.
  - Parameter: `name` (required topic name).
  - Optional properties: `PartitionCount` (default 1), `ReplicationFactor` (default 1).
  - Example: `[KsqlTopic("orders")]` / `[KsqlTopic("orders", PartitionCount=3, ReplicationFactor=2)]`
- `[KsqlTimestamp]`: treats the property as event time.
  - Target types: `DateTime` or `DateTimeOffset` (UTC assumed).
  - Note: maps to KSQL `TIMESTAMP`.
- `[KsqlDecimal(precision, scale)]`: specifies precision and scale for decimals.
  - Parameters: `precision` total digits, `scale` fraction digits.
  - Example: `[KsqlDecimal(18, 4)]`
- `[KsqlKey(order)]`: sets order in composite keys.
  - Parameter: `order` integer ≥ 0; smaller numbers come first.
  - Example: `Broker` as 0, `Symbol` as 1.
- `[KsqlIgnore]`: excludes a property from schema and messaging.
  - Use for internal calculations or temporary notes.
- `[KsqlTable]`: treat the entity as a table (stream is default).
  - Only add when table behavior is needed.

### Context and builder
- `KsqlContextBuilder.Create()`: create the builder.
- `.UseConfiguration(IConfiguration cfg)`: pass configuration.
- `.UseSchemaRegistry(string url)`: configure Schema Registry.
- `.EnableLogging(ILoggerFactory lf)`: enable logging.
- `.BuildContext<TContext>()`: create `IKsqlContext`.

### Fluent API (model registration)
- `ModelBuilder.Entity<T>(readOnly=false, writeOnly=false)`: register a type.
- `.ToQuery(Func<IQueryBuilder,IQueryBuilder> build)`: define a view.
- `From<TSource>()`: specify a source.
- `Join<TRight>(expr)`: join related data.
- `Where(expr)`: filter by condition.
- `Select(selector)`: define output shape.

### Event operations (send/receive)
- `IKsqlContext.Set<T>() -> IEventSet<T>`: obtain a typed set.
- `IEventSet<T>.AddAsync(T entity, CancellationToken? ct=null)`: send.
- `IEventSet<T>.ForEachAsync(Func<T,Task> handler, CancellationToken? ct=null)`: consume.

### TimeBucket (read/write bars)
- `ctx.TimeBucket.Get<T>(Period.Minutes(n)) -> TimeBucket<T>`: prepare read for a tumbling period.
  - `.ToListAsync(string[] keyParts, CancellationToken ct) -> Task<List<T>>`: fetch by key prefix (e.g., `[broker, symbol]` or `[broker, symbol, bucketStartUtc]`).
- `ctx.TimeBucket.Set<T>(Period.Minutes(n)) -> TimeBucketWriter<T>`: prepare write for a tumbling period.
  - `.AddAsync(T row, CancellationToken ct = default)`: send a bar using the configured producer.
- Notes: minutes-or-greater only; seconds are invalid for this API. Topics: `{topic}_{period}_live` (>=1m) / `{topic}_1s_final` (1s).
- See: `docs/api/timebucket.md` for end-to-end examples and details (aligns with `docs/chart.md`).

### Error handling and DLQ
- `IEventSet<T>.WithRetry(opts)`: configure retry policy.
- `IEventSet<T>.OnError(handler)`: set failure handling.
- `IDlqClient.ReadAsync(CancellationToken? ct=null) -> IAsyncEnumerable<DlqRecord>`: read the DLQ.

### Core interfaces
- `IKsqlContext`: manages KSQL interaction.
- `IEventSet<T>`: provides typed operations.
- `IDlqClient`: reads the DLQ.
- `ITableCache<T>`: provides table caching.

### Key configuration keys (appsettings.json)
- `KsqlDsl.Common.BootstrapServers`: Kafka bootstrap servers.
- `KsqlDsl.SchemaRegistry.Url`: Schema Registry URL.
- `KsqlDsl.KsqlDbUrl`: ksqlDB URL.
- `KsqlDsl.DlqTopicName`: DLQ topic name.
- `KsqlDsl.DeserializationErrorPolicy`: policy for deserialization errors.
