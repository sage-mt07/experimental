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

public class TimeBucketTests
{
    private class Rate
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime BucketStart { get; set; }
        public decimal Value { get; set; }
    }

    private static (MappingRegistry map, InMemoryRocks rocks, TimeBucket<Rate> bucket) CreateBucket(Period? p = null)
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
        var rocks = new InMemoryRocks(TimeSpan.FromMilliseconds(1));
        var ctx = new TestContext(map, rocks);
        var period = p ?? Period.Minutes(5);
        var bucket = TimeBucket.Get<Rate>(ctx, period);
        return (map, rocks, bucket);
    }

    private static void Seed(MappingRegistry map, InMemoryRocks rocks)
    {
        var m = map.GetMapping(typeof(Rate));
        var rows = new[]
        {
            new Rate{ Broker="B", Symbol="S", BucketStart=new DateTime(2025,8,23,10,0,0,DateTimeKind.Utc), Value=1 },
            new Rate{ Broker="B", Symbol="S", BucketStart=new DateTime(2025,8,23,10,5,0,DateTimeKind.Utc), Value=2 },
            new Rate{ Broker="B", Symbol="X", BucketStart=new DateTime(2025,8,23,10,0,0,DateTimeKind.Utc), Value=3 }
        };
        foreach (var r in rows)
        {
            var key = m.FormatKeyForPrefix(m.ExtractAvroKey(r));
            var val = m.ExtractAvroValue(r);
            rocks.Add("rate_5m_live", key, val);
        }
        var live = new Rate { Broker = "B", Symbol = "S", BucketStart = new DateTime(2025, 8, 23, 10, 10, 0, DateTimeKind.Utc), Value = 4 };
        var lkey = m.FormatKeyForPrefix(m.ExtractAvroKey(live));
        var lval = m.ExtractAvroValue(live);
        rocks.Add("rate_5m_live", lkey, lval);
    }

    [Fact]
    public void TimeBucket_ResolvesTopicName_FromPeriod_AndPoco()
    {
        var (_, _, bucket) = CreateBucket();
        Assert.Null(bucket.FinalTopicName);
        Assert.Equal("rate_5m_live", bucket.LiveTopicName);
    }

    [Fact]
    public void TopicResolver_Maps_1wk_To_Rate_1wk_Final()
    {
        var (_, _, bucket) = CreateBucket(Period.Week());
        Assert.Null(bucket.FinalTopicName);
        Assert.Equal("rate_1wk_live", bucket.LiveTopicName);
    }

    [Fact]
    public async Task TimeBucket_1s_Uses_Final_Only()
    {
        var (map, rocks, bucket) = CreateBucket(Period.Seconds(1));
        var m = map.GetMapping(typeof(Rate));
        var row = new Rate { Broker = "B", Symbol = "S", BucketStart = new DateTime(2025, 8, 23, 10, 0, 0, DateTimeKind.Utc), Value = 1 };
        var key = m.FormatKeyForPrefix(m.ExtractAvroKey(row));
        var val = m.ExtractAvroValue(row);
        rocks.Add("rate_1s_final", key, val);
        var list = await bucket.ToListAsync(new[] { "B", "S" }, CancellationToken.None);
        Assert.Single(list);
        Assert.Equal("rate_1s_final", bucket.FinalTopicName);
        Assert.Null(bucket.LiveTopicName);
    }

    [Fact]
    public async Task ToListAsync_ReturnsRows_WithBrokerSymbolPrefix()
    {
        var (map, rocks, bucket) = CreateBucket();
        Seed(map, rocks);
        var list = await bucket.ToListAsync(new[] { "B", "S" }, CancellationToken.None);
        Assert.Equal(3, list.Count);
    }

    [Fact]
    public async Task ToListAsync_ReturnsSingleRow_WithBucketStart()
    {
        var (map, rocks, bucket) = CreateBucket();
        Seed(map, rocks);
        var m = map.GetMapping(typeof(Rate));
        var sample = new Rate { Broker = "B", Symbol = "S", BucketStart = new DateTime(2025, 8, 23, 10, 0, 0, DateTimeKind.Utc) };
        var key = m.FormatKeyForPrefix(m.ExtractAvroKey(sample));
        var parts = key.Split(KeyValueTypeMapping.KeySep);
        var list = await bucket.ToListAsync(parts, CancellationToken.None);
        Assert.Single(list);
        Assert.Equal(1, list[0].Value);
    }

    [Fact]
    public async Task ToListAsync_NoRows_Throws()
    {
        var (map, rocks, bucket) = CreateBucket();
        Seed(map, rocks);
        await Assert.ThrowsAsync<InvalidOperationException>(() => bucket.ToListAsync(new[] { "B", "Y" }, CancellationToken.None));
    }

    [Fact]
    public async Task ToListAsync_ReturnsFromLiveTopic()
    {
        var (map, rocks, bucket) = CreateBucket();
        Seed(map, rocks);
        var list = await bucket.ToListAsync(new[] { "B", "S", "2025-08-23T10:10:00Z" }, CancellationToken.None);
        Assert.Single(list);
        Assert.Equal(4, list[0].Value);
    }

    [Fact]
    public async Task ToListAsync_FilterTooLong_Throws()
    {
        var (_, _, bucket) = CreateBucket();
        await Assert.ThrowsAsync<ArgumentException>(() => bucket.ToListAsync(new[] { "a", "b", "c", "d" }, CancellationToken.None));
    }

    [Fact]
    public async Task ToListAsync_Cancellation_StopsEarly()
    {
        var (map, rocks, bucket) = CreateBucket();
        Seed(map, rocks);
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAsync<TaskCanceledException>(() => bucket.ToListAsync(new[] { "B" }, cts.Token));
    }

    [Fact]
    public async Task TimeBucket_Get_1wk_ToListAsync_Prefix()
    {
        var (map, rocks, bucket) = CreateBucket(Period.Week());
        var m = map.GetMapping(typeof(Rate));
        var rows = new[]
        {
            new Rate{ Broker="B", Symbol="S", BucketStart=new DateTime(2025,8,11,0,0,0,DateTimeKind.Utc), Value=1 },
            new Rate{ Broker="B", Symbol="S", BucketStart=new DateTime(2025,8,18,0,0,0,DateTimeKind.Utc), Value=2 },
            new Rate{ Broker="B", Symbol="X", BucketStart=new DateTime(2025,8,11,0,0,0,DateTimeKind.Utc), Value=3 }
        };
        foreach (var r in rows)
        {
            var key = m.FormatKeyForPrefix(m.ExtractAvroKey(r));
            var val = m.ExtractAvroValue(r);
            rocks.Add("rate_1wk_live", key, val);
        }
        var live = new Rate { Broker = "B", Symbol = "S", BucketStart = new DateTime(2025, 8, 25, 0, 0, 0, DateTimeKind.Utc), Value = 4 };
        var lkey = m.FormatKeyForPrefix(m.ExtractAvroKey(live));
        var lval = m.ExtractAvroValue(live);
        rocks.Add("rate_1wk_live", lkey, lval);
        var list = await bucket.ToListAsync(new[] { "B", "S" }, CancellationToken.None);
        Assert.Equal(3, list.Count);
    }

    [Fact]
    public async Task BucketStart_IsFloored_To_Period_Boundary()
    {
        var (map, rocks, bucket) = CreateBucket();
        Seed(map, rocks);
        var list = await bucket.ToListAsync(new[] { "B", "S", "2025-08-23T10:03:00Z" }, CancellationToken.None);
        Assert.Single(list);
        Assert.Equal(new DateTime(2025, 8, 23, 10, 0, 0, DateTimeKind.Utc), list[0].BucketStart);
    }

    private class InMemoryRocks
    {
        private readonly Dictionary<string, Dictionary<string, object>> _data = new();
        private readonly TimeSpan _delay;
        public InMemoryRocks(TimeSpan? delay = null) => _delay = delay ?? TimeSpan.Zero;
        public void Add(string topic, string key, object value)
        {
            if (!_data.TryGetValue(topic, out var d))
                _data[topic] = d = new();
            d[key] = value;
        }
        public async IAsyncEnumerable<(string Key, object Value)> RangeScanAsync(string topic, string prefix, [EnumeratorCancellation] CancellationToken ct)
        {
            if (_data.TryGetValue(topic, out var d))
            {
                foreach (var kv in d)
                {
                    if (!kv.Key.StartsWith(prefix, StringComparison.Ordinal))
                        continue;
                    if (_delay > TimeSpan.Zero)
                        await Task.Delay(_delay, ct);
                    ct.ThrowIfCancellationRequested();
                    yield return (kv.Key, kv.Value);
                }
            }
        }
    }

    private class TestContext : ITimeBucketContext
    {
        private readonly MappingRegistry _map;
        private readonly InMemoryRocks _rocks;
        public TestContext(MappingRegistry map, InMemoryRocks rocks)
        {
            _map = map;
            _rocks = rocks;
        }
        public ITimeBucketSet<T> Set<T>(string topic, Period period) where T : class
            => new TestSet<T>(_map, _rocks, topic, period);
    }

    private class TestSet<T> : ITimeBucketSet<T> where T : class
    {
        private readonly MappingRegistry _map;
        private readonly InMemoryRocks _rocks;
        private readonly string _topic;
        private readonly Period _period;
        public TestSet(MappingRegistry map, InMemoryRocks rocks, string topic, Period period)
        {
            _map = map;
            _rocks = rocks;
            _topic = topic;
            _period = period;
        }
        public async Task<List<T>> ToListAsync(IReadOnlyList<string> filter, CancellationToken ct)
        {
            var map = _map.GetMapping(typeof(T));
            var pkCols = map.KeyProperties;
            if (filter.Count > pkCols.Length)
                throw new ArgumentException($"Filter parts {filter.Count} exceed PK length {pkCols.Length}");
            var parts = new string[filter.Count];
            for (int i = 0; i < filter.Count; i++) parts[i] = filter[i];
            if (parts.Length == pkCols.Length && pkCols[^1].PropertyInfo?.Name == "BucketStart")
            {
                if (DateTime.TryParse(parts[^1], out var dt))
                    parts[^1] = Periods.FloorUtc(dt, _period).ToString("yyyyMMdd'T'HHmmssfff'Z'", CultureInfo.InvariantCulture);
            }
            var prefix = string.Join(KeyValueTypeMapping.KeySep, parts);
            var list = new List<T>();
            await foreach (var (key, val) in _rocks.RangeScanAsync(_topic, prefix, ct))
            {
                ct.ThrowIfCancellationRequested();
                var poco = (T)map.CombineFromStringKeyAndAvroValue(key, val, typeof(T));
                list.Add(poco);
            }
            if (list.Count == 0)
                throw new InvalidOperationException("No rows matched the filter.");
            return list;
        }
    }
}
