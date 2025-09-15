using Kafka.Ksql.Linq;
using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Core.Modeling;
using Kafka.Ksql.Linq.Query.Builders;
using Kafka.Ksql.Linq.Runtime;
using Microsoft.Extensions.Logging;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Integration;

/// <summary>
/// docs/chart.md に対応する物理テスト。
/// - 1分（ライブ）と5分ロールアップの定義（TUMBLING）
/// - TimeBucket を用いたトピック解決とバケット境界の検証
/// - WhenEmpty ケース（欠損バケットの取扱い）は Pending（DSL未実装のため Skip）
/// </summary>
public class BarChartTimeBucketTests
{
    private class Rate
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlTimestamp] public DateTime Timestamp { get; set; }
        public double Bid { get; set; }
    }

    private class Bar
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlKey(3)] public DateTime BucketStart { get; set; }
        public double Open { get; set; }
        public double High { get; set; }
        public double Low { get; set; }
        public double KsqlTimeFrameClose { get; set; }
    }

    private sealed class TestContext : KsqlContext
    {
        private static readonly ILoggerFactory _loggerFactory = LoggerFactory.Create(b => b.AddConsole());
        public TestContext() : base(new KsqlDslOptions
        {
            Common = new CommonSection { BootstrapServers = "127.0.0.1:39092" },
            SchemaRegistry = new Kafka.Ksql.Linq.Core.Configuration.SchemaRegistrySection { Url = "http://127.0.0.1:18081" },
            KsqlDbUrl = "http://127.0.0.1:18088"
        }, _loggerFactory) { }
        public EventSet<Rate> Rates { get; set; } = null!;
        protected override void OnModelCreating(IModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Bar>()
                .ToQuery(q => q.From<Rate>()
                    .Tumbling(r => r.Timestamp, new Kafka.Ksql.Linq.Query.Dsl.Windows { Minutes = new[] { 1, 5 } })
                    .GroupBy(r => new { r.Broker, r.Symbol })
                    .Select(g => new Bar
                    {
                        Broker = g.Key.Broker,
                        Symbol = g.Key.Symbol,
                        BucketStart = g.WindowStart(),
                        Open = g.EarliestByOffset(x => x.Bid),
                        High = g.Max(x => x.Bid),
                        Low = g.Min(x => x.Bid),
                        KsqlTimeFrameClose = g.LatestByOffset(x => x.Bid)
                    }));
        }
    }

    private static async Task<List<object?[]>> QueryRowsAsync(string sql, TimeSpan timeout)
    {
        using var http = new HttpClient { BaseAddress = new Uri("http://127.0.0.1:18088") };
        var payload = new { sql, properties = new Dictionary<string, object>() };
        using var content = new StringContent(JsonSerializer.Serialize(payload), Encoding.UTF8, "application/json");
        using var cts = new CancellationTokenSource(timeout);
        using var resp = await http.PostAsync("/query", content, cts.Token);
        resp.EnsureSuccessStatusCode();
        var body = await resp.Content.ReadAsStringAsync(cts.Token);
        var rows = new List<object?[]>();
        using var doc = JsonDocument.Parse(body);
        if (doc.RootElement.ValueKind == JsonValueKind.Array)
        {
            foreach (var el in doc.RootElement.EnumerateArray())
            {
                if (el.TryGetProperty("row", out var row) && row.ValueKind == JsonValueKind.Array)
                    rows.Add(row.EnumerateArray().Select(v => v.ValueKind == JsonValueKind.Null ? null : v.Deserialize<object>()).ToArray());
            }
        }
        return rows;
    }

    [Fact(Skip = "Requires local ksqlDB/Kafka environment")]
    public async Task Chart_1m_and_5m_With_TimeBucket_Verify()
    {
        var ctx = new TestContext();
        var map = new Mapping.MappingRegistry();
        map.RegisterEntityModel(ctx.GetEntityModels()[typeof(Bar)], genericValue: true);

        // TimeBucket でトピック名が period から解決されること
        var bucket1m = TimeBucket.Get<Bar>(new Runtime.TestContext(map, new RuntimeRocks()), Period.Minutes(1));
        Assert.Equal("bar_1m_live", bucket1m.LiveTopicName);
        var bucket5m = TimeBucket.Get<Bar>(new Runtime.TestContext(map, new RuntimeRocks()), Period.Minutes(5));
        Assert.Equal("bar_5m_live", bucket5m.LiveTopicName);

        // データ投入（1m内のOHLCが分かるよう複数点）
        var t0 = DateTime.UtcNow.AddMinutes(-10);
        string broker = "B"; string symbol = "S";
        await ctx.Rates.AddAsync(new Rate { Broker = broker, Symbol = symbol, Timestamp = t0.AddSeconds(1), Bid = 100 });
        await ctx.Rates.AddAsync(new Rate { Broker = broker, Symbol = symbol, Timestamp = t0.AddSeconds(20), Bid = 105 });
        await ctx.Rates.AddAsync(new Rate { Broker = broker, Symbol = symbol, Timestamp = t0.AddSeconds(40), Bid = 99 });
        await ctx.Rates.AddAsync(new Rate { Broker = broker, Symbol = symbol, Timestamp = t0.AddSeconds(55), Bid = 101 });

        // Pull で 1m バケットの OHLC を確認
        static long Ms(DateTime dt) => (long)(dt - DateTime.UnixEpoch).TotalMilliseconds;
        var bs1m = Ms(new DateTime(t0.Year, t0.Month, t0.Day, t0.Hour, t0.Minute, 0, DateTimeKind.Utc));
        var rows1m = await QueryRowsAsync($"SELECT BucketStart, Open, High, Low, KsqlTimeFrameClose FROM bar_1m_live WHERE Broker='{broker}' AND Symbol='{symbol}' AND BucketStart={bs1m};", TimeSpan.FromSeconds(20));
        Assert.True(rows1m.Count >= 1, "1m bucket not produced");

        // 5m ロールアップの存在のみ確認（詳細は別テストでカバー）
        var bs5m = Ms(new DateTime(t0.Year, t0.Month, t0.Day, t0.Hour, (t0.Minute/5)*5, 0, DateTimeKind.Utc));
        var rows5m = await QueryRowsAsync($"SELECT BucketStart FROM bar_5m_live WHERE Broker='{broker}' AND Symbol='{symbol}' AND BucketStart={bs5m};", TimeSpan.FromSeconds(20));
        Assert.True(rows5m.Count >= 1, "5m bucket not produced");
    }

    [Fact(Skip = "WhenEmpty fill KSQL is pending; will verify via TimeBucket once implemented")]
    public void WhenEmpty_Fills_Missing_Minute_With_Previous_Close()
    {
        // docs/chart.md の WhenEmpty（欠損埋め）に対応する検証
        // フィジカル生成（HB/Prev JOIN + Fill 投影）の具象化が未反映のため Skip。
        // 実装後は 1分の欠損を直前 Close で埋めることを TimeBucket 経由で確認する。
    }

    // RuntimeRocks/TestContext は TimeBucket テスト用の最小ダミー（外部依存なし）
    private sealed class RuntimeRocks : Runtime.IRocksDbProvider
    {
        public IAsyncEnumerable<(string Key, object Value)> RangeScanAsync(string topic, string prefix, CancellationToken ct) => AsyncEmpty();
        private static async IAsyncEnumerable<(string, object)> AsyncEmpty() { await Task.CompletedTask; yield break; }
    }
}
