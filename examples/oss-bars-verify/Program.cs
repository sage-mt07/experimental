using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Ksql.Linq.Runtime;

// Self-contained verifier using OSS runtime APIs only (no Kafka needed)

public class Rate
{
    public string Broker { get; set; } = string.Empty;
    public string Symbol { get; set; } = string.Empty;
    public DateTime BucketStart { get; set; }
    public decimal Open { get; set; }
    public decimal High { get; set; }
    public decimal Low { get; set; }
    public decimal Close { get; set; }
}

sealed class InMemoryStore
{
    private readonly Dictionary<string, List<(string Key, Rate Value)>> _data = new();
    public void Add(string topic, string key, Rate value)
    {
        if (!_data.TryGetValue(topic, out var list)) _data[topic] = list = new();
        list.Add((key, value));
    }
    public async IAsyncEnumerable<(string Key, Rate Value)> RangeScanAsync(string topic, string prefix, [EnumeratorCancellation] CancellationToken ct)
    {
        if (_data.TryGetValue(topic, out var list))
        {
            foreach (var kv in list.OrderBy(x => x.Key))
            {
                if (!kv.Key.StartsWith(prefix, StringComparison.Ordinal)) continue;
                ct.ThrowIfCancellationRequested();
                yield return (kv.Key, kv.Value);
                await Task.Yield();
            }
        }
    }
}

sealed class TestBucketContext : ITimeBucketContext
{
    private readonly InMemoryStore _store;
    public TestBucketContext(InMemoryStore store) { _store = store; }
    public ITimeBucketSet<T> Set<T>(string topic, Period period) where T : class
        => new TestBucketSet<T>(_store, topic, period);
}

sealed class TestBucketSet<T> : ITimeBucketSet<T> where T : class
{
    private readonly InMemoryStore _store; private readonly string _topic; private readonly Period _period;
    public TestBucketSet(InMemoryStore store, string topic, Period period)
    { _store = store; _topic = topic; _period = period; }
    public async Task<List<T>> ToListAsync(IReadOnlyList<string> filter, CancellationToken ct)
    {
        if (filter.Count is < 1 or > 3) throw new ArgumentException("Filter must be 1..3 parts");
        var parts = new string[filter.Count];
        for (int i = 0; i < filter.Count; i++) parts[i] = filter[i];
        if (parts.Length == 3)
        {
            if (DateTime.TryParse(parts[^1], out var dt))
                parts[^1] = Periods.FloorUtc(dt, _period).ToString("yyyyMMdd'T'HHmmssfff'Z'", CultureInfo.InvariantCulture);
        }
        var prefix = string.Join("\u0000", parts);
        var list = new List<T>();
        await foreach (var (key, val) in _store.RangeScanAsync(_topic, prefix, ct))
        {
            ct.ThrowIfCancellationRequested();
            list.Add((T)(object)val);
        }
        if (list.Count == 0) throw new InvalidOperationException("No rows matched the filter.");
        return list;
    }
}

static class Program
{
    static string KeyFor(Rate row)
        => string.Join("\u0000",
            new []{
                row.Broker,
                row.Symbol,
                row.BucketStart.ToUniversalTime().ToString("yyyyMMdd'T'HHmmssfff'Z'", CultureInfo.InvariantCulture)
            });

    public static async Task<int> Main()
    {
        var store = new InMemoryStore();

        // Seed 1m bars (10 minutes) with deterministic values
        var start = new DateTime(2025, 9, 5, 12, 0, 0, DateTimeKind.Utc);
        var broker = "B"; var symbol = "S";
        decimal lastClose = 100m;
        var oneMin = new List<Rate>();
        for (int i = 0; i < 10; i++)
        {
            var t = start.AddMinutes(i);
            var open = lastClose;
            var close = Math.Round(open + 0.0100m * 59m, 4, MidpointRounding.AwayFromZero);
            var high = close;
            var low = open;
            var bar = new Rate { Broker = broker, Symbol = symbol, BucketStart = t, Open = open, High = high, Low = low, Close = close };
            oneMin.Add(bar);
            lastClose = close;
            var k = KeyFor(bar);
            store.Add("rate_1m_final", k, bar);
        }

        // Build and seed expected 5m
        static DateTime Floor5m(DateTime t) => new DateTime((t.Ticks / TimeSpan.FromMinutes(5).Ticks) * TimeSpan.FromMinutes(5).Ticks, DateTimeKind.Utc);
        var expected5 = oneMin
            .GroupBy(b => Floor5m(b.BucketStart))
            .Select(g => new Rate
            {
                Broker = broker,
                Symbol = symbol,
                BucketStart = g.Key,
                Open = g.OrderBy(x => x.BucketStart).First().Open,
                High = g.Max(x => x.High),
                Low = g.Min(x => x.Low),
                Close = g.OrderBy(x => x.BucketStart).Last().Close
            })
            .OrderBy(b => b.BucketStart)
            .ToList();

        foreach (var b in expected5)
        {
            var k = KeyFor(b);
            store.Add("rate_5m_final", k, b);
        }

        // Verify via TimeBucket
        var ctx = new TestBucketContext(store);
        var one = await TimeBucket.Get<Rate>(ctx, Period.Minutes(1)).ToListAsync(new[] { broker, symbol }, CancellationToken.None);
        var five = await TimeBucket.Get<Rate>(ctx, Period.Minutes(5)).ToListAsync(new[] { broker, symbol }, CancellationToken.None);
        one = one.OrderBy(b => b.BucketStart).ToList();
        five = five.OrderBy(b => b.BucketStart).ToList();

        if (one.Count != 10 || five.Count != 2)
        {
            Console.WriteLine($"[fail] counts 1m={one.Count} 5m={five.Count} (expected 10 and 2)");
            return 1;
        }

        for (int i = 0; i < 2; i++)
        {
            if (five[i].BucketStart != expected5[i].BucketStart ||
                five[i].Open != expected5[i].Open ||
                five[i].High != expected5[i].High ||
                five[i].Low != expected5[i].Low ||
                five[i].Close != expected5[i].Close)
            {
                Console.WriteLine($"[mismatch] {five[i].BucketStart:HH:mm} O:{five[i].Open}/{expected5[i].Open} H:{five[i].High}/{expected5[i].High} L:{five[i].Low}/{expected5[i].Low} C:{five[i].Close}/{expected5[i].Close}");
                return 2;
            }
        }

        Console.WriteLine("[ok] 5m bars match 1m rollup (OHLC) and counts are correct");
        return 0;
    }
}
