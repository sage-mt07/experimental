# Bar Generation Quickstart for Practitioners
This memo shows how to generate multi-timeframe bars from ticks with the shortest possible workflow.

Target readers
- Engineers who already operate Kafka and ksqlDB in production.
- People who prioritize running the flow over digging into implementation details.

What you can do
- Generate multiple bar timeframes from ticks in one pass (for example 1m/5m/15m/1h/1d).
- Stabilize day/week boundaries by joining with a MarketSchedule (business calendar).
- Declare OHLC aggregations inside `Select` and run them as-is.
- Materialize tables into RocksDB (internal storage) and fetch them quickly with `ToListAsync()`.

## Verify in five steps
1) Configure endpoints
- Start Kafka / ksqlDB / Schema Registry.
- Set the minimum endpoints in `appsettings.json`:
  - `KsqlDsl:Common:BootstrapServers`
  - `KsqlDsl:SchemaRegistry:Url`
  - `KsqlDsl:KsqlDbUrl`

2) Build the context and connect
```csharp
var cfg = new ConfigurationBuilder()
  .AddJsonFile("appsettings.json") // load connection settings
  .Build();
var ctx = KsqlContextBuilder.Create()
  .UseConfiguration(cfg)            // apply configuration
  .BuildContext<MyAppContext>();
```

3) Map `Rate` to topic `rates`
```csharp
[KsqlTopic("rates")]
public class Rate
{
  [KsqlKey(0)] public string Broker { get; set; } = "";
  [KsqlKey(1)] public string Symbol { get; set; } = "";
  [KsqlTimestamp] public DateTime Timestamp { get; set; }
  public double Bid { get; set; }
}
```

4) Define multiple bars including day/week boundaries
```csharp
modelBuilder.Entity<Bar>().ToQuery(q => q
  .From<Rate>()
  .TimeFrame<MarketSchedule>((r, s) =>
         r.Broker == s.Broker
      && r.Symbol == s.Symbol
      && s.Open <= r.Timestamp && r.Timestamp < s.Close,
      dayKey: s => s.MarketDate)
  .Tumbling(r => r.Timestamp, new Windows { Minutes = new[]{ 1, 5, 15 }, Days = new[]{ 1 } })
  .GroupBy(r => new { r.Broker, r.Symbol })
  .Select(g => new Bar {
    Broker = g.Key.Broker,
    Symbol = g.Key.Symbol,
    BucketStart = g.WindowStart(),
    Open  = g.EarliestByOffset(x => x.Bid),
    High  = g.Max(x => x.Bid),
    Low   = g.Min(x => x.Bid),
    Close = g.LatestByOffset(x => x.Bid)
  }));
```

5) Send and receive
- Send through the stream/table using the same API.
```csharp
await ctx.Set<Rate>().AddAsync(new Rate {
  Broker = "B1", Symbol = "S1", Timestamp = DateTime.UtcNow, Bid = 100
});
```
- Consume the stream with `ForEachAsync()`.
```csharp
await ctx.Set<Bar>().ForEachAsync(b => { Console.WriteLine(b.Symbol); return Task.CompletedTask; });
```
- Fetch the table from RocksDB with `ToListAsync()`.
```csharp
var list = await ctx.Set<Bar>().ToListAsync();
```

## Usage tips
Keep these points in mind while coding:
- Include `g.WindowStart()` exactly once inside `Select`. Duplicates trigger errors.
- Add `dayKey` when creating daily-or-longer bars; it identifies the business day.
- All bars derive directly from the 1-second base. Even 5m and 15m come from the 1-second parent.
- Separate DAGs for finalized (`final`) and live (`live`) flows.
- Use `ToListAsync()` for tables and `ForEachAsync()` for streams.
- Latency varies by environment: usually 50–200 ms, 0.5–3 seconds right after startup.

## Automatic checks and how to fix errors
Internal rules catch most mistakes. When you see an error, check the following:
- `Windowed query requires exactly one WindowStart()`
  - Fix: include `g.WindowStart()` exactly once in `Select`. Watch for duplicates or omissions.
- `Windows ≥ 1 minute must be whole-minute multiples`
  - Fix: choose integer-minute sizes (for example 1m, 5m, 15m).
- Other window-size errors (for example "Windows must be multiples of the base unit")
  - Fix: avoid second-level sizes; use common values like 1m/5m/15m/1h/1d.
Handle these and most validation errors disappear. Next review the naming rules.

## Naming rules
- Format: `<entity>_<timeframe>_(live|final)` such as `bar_1m_live`, `bar_1d_live`.
- Timeframe abbreviations: `s` seconds, `m` minutes, `h` hours, `d` days, `mo` months.
- The `1s_final` table is the parent for higher frames.

## Troubleshooting
- If propagation is slow, wait a few seconds after startup and retry with short polling.
- If counts look low, check the `TimeFrame` conditions, `dayKey`, and `WindowStart` projection.
- If `ToListAsync()` throws, ensure the target is a table (streams do not support it).

Reference
- See `docs/chart.md` for full detail.

Checklist
- `bar_1m_live` receives records.
- When `ToListAsync()` returns zero rows, revisit `TimeFrame` and `WindowStart()`.
- `ksqlDB` `SHOW TABLES` lists `bar_1m_live` and `bar_1d_live`.
