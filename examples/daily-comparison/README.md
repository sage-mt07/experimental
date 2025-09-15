# Daily Comparison Sample

This example demonstrates a simple rate ingestion and daily aggregation using **Kafka.Ksql.Linq** only.
All settings including logging and Schema Registry configuration are read from
`appsettings.json` following `../../docs/configuration_reference.md`.
`MyKsqlContext.FromAppSettings()` builds the context directly from this file.
Bar and window definitions (1, 5, 60 minute bars and daily bars) are declared in `KafkaKsqlContext.OnModelCreating`.

```csharp
protected override void OnModelCreating(IModelBuilder modelBuilder)
{
    modelBuilder.WithWindow<Rate, MarketSchedule>(
        new[] { 1, 5, 60 },
        r => r.RateTimestamp,
        r => new { r.Broker, r.Symbol, Date = r.RateTimestamp.Date },
        s => new { s.Broker, s.Symbol, s.Date }
    )
    .Select<RateCandle>(w =>
    {
        dynamic key = w.Key;
        return new RateCandle
        {
            Broker = key.Broker,
            Symbol = key.Symbol,
            BarTime = w.BarStart,
            Open = w.Source.OrderBy(x => x.RateTimestamp).First().Bid,
            High = w.Source.Max(x => x.Bid),
            Low = w.Source.Min(x => x.Bid),
            Close = w.Source.OrderByDescending(x => x.RateTimestamp).First().Bid
        };
    });
}
```

`Aggregator` simply queries the aggregated `RateCandle` and `DailyComparison` sets.

## Usage

1. Start the local Kafka stack:
   ```bash
   docker compose up -d
   ```
2. Run the rate sender which also performs aggregation. It uses `MyKsqlContext.FromAppSettings()`:
   ```bash
   dotnet run --project RateSender
   ```
   This sends a rate every second (100 messages total) and stores the daily comparison.
3. Display aggregated rows using the same context implementation:
    ```bash
    dotnet run --project ComparisonViewer
    ```
    This will print daily comparisons along with a "Previous Day Change" column
    showing day-over-day change calculated from the latest rate and the most
    recent available daily close (0 if unavailable).

See the repository root README for package installation and local setup details.
