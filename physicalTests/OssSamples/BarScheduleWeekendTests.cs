using Kafka.Ksql.Linq;
using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Core.Modeling;
using Microsoft.Extensions.Logging;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using Xunit;
using System;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;

namespace Kafka.Ksql.Linq.Tests.Integration;

public class BarScheduleWeekendTests
{
    [KsqlTopic("deduprates")] 
    public class Rate
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlTimestamp] public DateTime Timestamp { get; set; }
        public double Bid { get; set; }
    }

    [KsqlTopic("marketschedule")] 
    public class MarketSchedule
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        public DateTime Open { get; set; }
        public DateTime Close { get; set; }
        public DateTime MarketDate { get; set; }
    }

    public class Bar1dLive
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlKey(3)] public DateTime BucketStart { get; set; }
        public double Open { get; set; }
        public double High { get; set; }
        public double Low { get; set; }
        public double KsqlTimeFrameClose { get; set; }
    }

    public class Bar1wkFinal
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
        private static readonly ILoggerFactory _lf = LoggerFactory.Create(b => b.AddConsole().SetMinimumLevel(LogLevel.Information));
        public TestContext() : base(new KsqlDslOptions
        {
            Common = new CommonSection { BootstrapServers = "127.0.0.1:39092" },
            SchemaRegistry = new Kafka.Ksql.Linq.Core.Configuration.SchemaRegistrySection { Url = "http://127.0.0.1:18081" },
            KsqlDbUrl = "http://127.0.0.1:18088"
        }, _lf) { }
        protected override bool SkipSchemaRegistration => false;
        public EventSet<Rate> Rates { get; set; } = null!;
        public EventSet<MarketSchedule> Schedules { get; set; } = null!;
        protected override void OnModelCreating(IModelBuilder mb)
        {
            mb.Entity<Bar1dLive>()
              .ToQuery(q => q.From<Rate>()
                .TimeFrame<MarketSchedule>((r, s) => r.Broker == s.Broker && r.Symbol == s.Symbol && s.Open <= r.Timestamp && r.Timestamp < s.Close,
                                           dayKey: s => s.MarketDate)
                .Tumbling(r => r.Timestamp, new Kafka.Ksql.Linq.Query.Dsl.Windows { Days = new[] { 1 } })
                .GroupBy(r => new { r.Broker, r.Symbol })
                .Select(g => new Bar1dLive
                {
                    Broker = g.Key.Broker,
                    Symbol = g.Key.Symbol,
                    BucketStart = g.WindowStart(),
                    Open = g.EarliestByOffset(x => x.Bid),
                    High = g.Max(x => x.Bid),
                    Low = g.Min(x => x.Bid),
                    KsqlTimeFrameClose = g.LatestByOffset(x => x.Bid)
                }));

            mb.Entity<Bar1wkFinal>()
              .ToQuery(q => q.From<Rate>()
                .TimeFrame<MarketSchedule>((r, s) => r.Broker == s.Broker && r.Symbol == s.Symbol && s.Open <= r.Timestamp && r.Timestamp < s.Close,
                                           dayKey: s => s.MarketDate)
                .Tumbling(r => r.Timestamp, new Kafka.Ksql.Linq.Query.Dsl.Windows { Days = new[] { 7 } })
                .GroupBy(r => new { r.Broker, r.Symbol })
                .Select(g => new Bar1wkFinal
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

    static async Task<int> CountRowsAsync(string sql, TimeSpan timeout)
    {
        using var http = new HttpClient { BaseAddress = new Uri("http://127.0.0.1:18088") };
        var payload = new { sql };
        var json = JsonSerializer.Serialize(payload);
        using var content = new StringContent(json, Encoding.UTF8, "application/json");
        using var cts = new CancellationTokenSource(timeout);
        using var resp = await http.PostAsync("/query", content, cts.Token);
        resp.EnsureSuccessStatusCode();
        var body = await resp.Content.ReadAsStringAsync(cts.Token);
        try
        {
            using var doc = JsonDocument.Parse(body);
            int cnt = 0;
            foreach (var el in doc.RootElement.EnumerateArray())
            {
                if (el.ValueKind == JsonValueKind.Object && el.TryGetProperty("row", out _)) cnt++;
            }
            return cnt;
        }
        catch { return 0; }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task WeekdaysOnly_DailyBars_With_MarketSchedule()
    {
        await PhysicalTestEnv.KsqlHelpers.WaitForKsqlReadyAsync("http://127.0.0.1:18088", TimeSpan.FromSeconds(180), graceMs: 2000);
        await using var ctx = new TestContext();

        using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "127.0.0.1:39092" }).Build())
        {
            try { await admin.CreateTopicsAsync(new[] { new TopicSpecification { Name = "deduprates", NumPartitions = 1, ReplicationFactor = 1 }, new TopicSpecification { Name = "marketschedule", NumPartitions = 1, ReplicationFactor = 1 } }); } catch { }
        }

        // Create schedule: Mon-Fri only (UTC), 09:00-15:00
        var monday = DateTime.UtcNow.Date;
        // align to Monday of current week
        int delta = ((int)DayOfWeek.Monday - (int)monday.DayOfWeek + 7) % 7;
        monday = monday.AddDays(delta);
        var days = Enumerable.Range(0, 7).Select(i => monday.AddDays(i)).ToArray();
        foreach (var d in days)
        {
            if (d.DayOfWeek is DayOfWeek.Saturday or DayOfWeek.Sunday) continue;
            await ctx.Schedules.AddAsync(new MarketSchedule
            {
                Broker = "B1",
                Symbol = "S1",
                MarketDate = d,
                Open = d.AddHours(9),
                Close = d.AddHours(15)
            });
        }

        // Produce rate: one tick at noon every day Mon-Sun
        foreach (var d in days)
        {
            await ctx.Rates.AddAsync(new Rate { Broker = "B1", Symbol = "S1", Timestamp = d.AddHours(12), Bid = 100 });
        }

        // Wait up to 60s for daily bars to appear
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(60);
        int count = 0;
        while (DateTime.UtcNow < deadline)
        {
            count = await CountRowsAsync("SELECT BucketStart, Open, High, Low, KsqlTimeFrameClose FROM bar_1d_live WHERE Broker='B1' AND Symbol='S1';", TimeSpan.FromSeconds(10));
            if (count >= 5) break;
            await Task.Delay(1000);
        }
        Assert.True(count >= 5 && count <= 5, $"Expected exactly 5 weekday daily bars, got {count}");

        // Weekly final should have 1 row for the week
        int wk = await CountRowsAsync("SELECT BucketStart, Open, High, Low, KsqlTimeFrameClose FROM bar_1wk_final WHERE Broker='B1' AND Symbol='S1';", TimeSpan.FromSeconds(10));
        Assert.True(wk >= 1, $"Expected weekly final bar, got {wk}");
    }
}
