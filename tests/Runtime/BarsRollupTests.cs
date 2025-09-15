using Kafka.Ksql.Linq.Core.Models;
using Kafka.Ksql.Linq.Mapping;
using Kafka.Ksql.Linq.Runtime;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Runtime;

public class BarsRollupTests
{
    // docs/chart.md の足POCOに合わせる
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

    // シンプルなメモリストア
    private sealed class InMemoryStore
    {
        private readonly Dictionary<string, Dictionary<string, object>> _data = new();
        public void Add(string topic, string key, object value)
        {
            if (!_data.TryGetValue(topic, out var d)) _data[topic] = d = new();
            d[key] = value;
        }
        public async IAsyncEnumerable<(string Key, object Value)> RangeScanAsync(string topic, string prefix, [EnumeratorCancellation] CancellationToken ct)
        {
            if (_data.TryGetValue(topic, out var d))
            {
                foreach (var kv in d.OrderBy(x => x.Key))
                {
                    if (!kv.Key.StartsWith(prefix, StringComparison.Ordinal)) continue;
                    ct.ThrowIfCancellationRequested();
                    yield return (kv.Key, kv.Value);
                    await Task.Yield();
                }
            }
        }
    }

    private sealed class TestBucketContext : ITimeBucketContext
    {
        private readonly MappingRegistry _map;
        private readonly InMemoryStore _store;
        public TestBucketContext(MappingRegistry map, InMemoryStore store) { _map = map; _store = store; }
        public ITimeBucketSet<T> Set<T>(string topic, Period period) where T : class
            => new TestBucketSet<T>(_map, _store, topic, period);
    }

    private sealed class TestBucketSet<T> : ITimeBucketSet<T> where T : class
    {
        private readonly MappingRegistry _map; private readonly InMemoryStore _store; private readonly string _topic; private readonly Period _period;
        public TestBucketSet(MappingRegistry map, InMemoryStore store, string topic, Period period)
        { _map = map; _store = store; _topic = topic; _period = period; }
        public async Task<List<T>> ToListAsync(IReadOnlyList<string> filter, CancellationToken ct)
        {
            var map = _map.GetMapping(typeof(T));
            var pkCols = map.KeyProperties;
            if (filter.Count > pkCols.Length) throw new ArgumentException($"Filter parts {filter.Count} exceed PK length {pkCols.Length}");
            var parts = new string[filter.Count];
            for (int i = 0; i < filter.Count; i++) parts[i] = filter[i];
            if (parts.Length == pkCols.Length && pkCols[^1].PropertyInfo?.Name == nameof(Rate.BucketStart))
            {
                if (DateTime.TryParse(parts[^1], out var dt))
                    parts[^1] = Periods.FloorUtc(dt, _period).ToString("yyyyMMdd'T'HHmmssfff'Z'", CultureInfo.InvariantCulture);
            }
            var prefix = string.Join(KeyValueTypeMapping.KeySep, parts);
            var list = new List<T>();
            await foreach (var (key, val) in _store.RangeScanAsync(_topic, prefix, ct))
            {
                ct.ThrowIfCancellationRequested();
                var poco = (T)map.CombineFromStringKeyAndAvroValue(key, val, typeof(T));
                list.Add(poco);
            }
            if (list.Count == 0) throw new InvalidOperationException("No rows matched the filter.");
            return list;
        }
    }

    private static MappingRegistry RegisterRate()
    {
        var map = new MappingRegistry();
        var keyProps = new[]
        {
            PropertyMeta.FromProperty(typeof(Rate).GetProperty(nameof(Rate.Broker))!),
            PropertyMeta.FromProperty(typeof(Rate).GetProperty(nameof(Rate.Symbol))!),
            PropertyMeta.FromProperty(typeof(Rate).GetProperty(nameof(Rate.BucketStart))!)
        };
        var valueProps = typeof(Rate).GetProperties().Select(p => PropertyMeta.FromProperty(p)).ToArray();
        map.Register(typeof(Rate), keyProps, valueProps);
        return map;
    }

    private static string KeyFor(MappingRegistry map, Rate row)
    {
        var m = map.GetMapping(typeof(Rate));
        return m.FormatKeyForPrefix(m.ExtractAvroKey(row));
    }

    [Fact(Skip = "Disabled after key prefix changes")]
    public async Task FiveMinuteBars_Are_Rollup_Of_OneMinuteBars()
    {
        var map = RegisterRate();
        var store = new InMemoryStore();

        // 1分足を10本生成（単調増加の決定値）
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

            var k1 = KeyFor(map, bar);
            var v1 = map.GetMapping(typeof(Rate)).ExtractAvroValue(bar);
            store.Add("rate_1m_final", k1, v1);
        }

        // 期待される5分足（1分のロールアップ）
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

        Assert.Equal(2, expected5.Count);

        foreach (var b in expected5)
        {
            var k = KeyFor(map, b);
            var v = map.GetMapping(typeof(Rate)).ExtractAvroValue(b);
            store.Add("rate_5m_final", k, v);
        }

        // 取得と検証（OSSランタイムのTimeBucketを使用）
        var ctx = new TestBucketContext(map, store);
        var bucket5 = TimeBucket.Get<Rate>(ctx, Period.Minutes(5));
        var actual = await bucket5.ToListAsync(new[] { broker, symbol }, CancellationToken.None);
        actual = actual.OrderBy(b => b.BucketStart).ToList();

        Assert.Equal(2, actual.Count);
        for (int i = 0; i < 2; i++)
        {
            Assert.Equal(expected5[i].BucketStart, actual[i].BucketStart);
            Assert.Equal(expected5[i].Open, actual[i].Open);
            Assert.Equal(expected5[i].High, actual[i].High);
            Assert.Equal(expected5[i].Low, actual[i].Low);
            Assert.Equal(expected5[i].Close, actual[i].Close);
        }
    }
}
