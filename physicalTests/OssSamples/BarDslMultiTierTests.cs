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
using System.Collections.Generic;
using System.Threading.Tasks;
using System;
using System.Linq;
using System.Threading;

namespace Kafka.Ksql.Linq.Tests.Integration;

public class BarDslMultiTierTests
{
    [KsqlTopic("deduprates")] 
    public class Rate
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlTimestamp] public DateTime Timestamp { get; set; }
        public double Bid { get; set; }
    }

    public class Bar
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
        private static readonly ILoggerFactory _loggerFactory = LoggerFactory.Create(b => b.AddConsole().SetMinimumLevel(LogLevel.Information));
        public TestContext() : base(new KsqlDslOptions
        {
            Common = new CommonSection { BootstrapServers = "127.0.0.1:39092" },
            SchemaRegistry = new Kafka.Ksql.Linq.Core.Configuration.SchemaRegistrySection { Url = "http://127.0.0.1:18081" },
            KsqlDbUrl = "http://127.0.0.1:18088"
        }, _loggerFactory) { }
        protected override bool SkipSchemaRegistration => false;
        public EventSet<Rate> Rates { get; set; } = null!;
        protected override void OnModelCreating(IModelBuilder modelBuilder)
        {
            // 多段（1m, 5m, 15m, 60m(=60m)）
            modelBuilder.Entity<Bar>()
                .ToQuery(q => q.From<Rate>()
                    .Tumbling(r => r.Timestamp, new Kafka.Ksql.Linq.Query.Dsl.Windows { Minutes = new[] { 1, 5, 15, 60 } })
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

    private static async Task<int> QueryStreamCountHttpAsync(string sql, int limit, TimeSpan timeout)
    {
        using var http = new HttpClient { BaseAddress = new Uri("http://127.0.0.1:18088") };
        var payload = new
        {
            sql,
            properties = new Dictionary<string, object>
            {
                ["ksql.streams.auto.offset.reset"] = "earliest"
            }
        };
        var json = JsonSerializer.Serialize(payload);
        using var content = new StringContent(json, Encoding.UTF8, "application/json");
        using var cts = new CancellationTokenSource(timeout);
        using var req = new HttpRequestMessage(HttpMethod.Post, "/query-stream") { Content = content };
        using var resp = await http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, cts.Token);
        resp.EnsureSuccessStatusCode();
        int count = 0;
        await using var stream = await resp.Content.ReadAsStreamAsync(cts.Token);
        using var reader = new System.IO.StreamReader(stream, Encoding.UTF8);
        while (!reader.EndOfStream && !cts.IsCancellationRequested)
        {
            var line = await reader.ReadLineAsync();
            if (line == null) break;
            if (line.IndexOf("\"row\"", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                count++;
                if (count >= limit) break;
            }
        }
        return count;
    }

    private static async Task<List<object?[]>> QueryRowsHttpAsync(string sql, TimeSpan timeout)
    {
        using var http = new HttpClient { BaseAddress = new Uri("http://127.0.0.1:18088") };
        var payload = new { sql, properties = new Dictionary<string, object>() };
        var json = JsonSerializer.Serialize(payload);
        using var content = new StringContent(json, Encoding.UTF8, "application/json");
        using var cts = new CancellationTokenSource(timeout);
        using var resp = await http.PostAsync("/query", content, cts.Token);
        resp.EnsureSuccessStatusCode();
        var body = await resp.Content.ReadAsStringAsync(cts.Token);
        var rows = new List<object?[]>();
        try
        {
            using var doc = JsonDocument.Parse(body);
            if (doc.RootElement.ValueKind == JsonValueKind.Array)
            {
                foreach (var el in doc.RootElement.EnumerateArray())
                {
                    if (el.ValueKind == JsonValueKind.Object && el.TryGetProperty("row", out var rowEl))
                    {
                        if (rowEl.TryGetProperty("columns", out var cols) && cols.ValueKind == JsonValueKind.Array)
                        {
                            var arr = new object?[cols.GetArrayLength()];
                            int idx = 0;
                            foreach (var c in cols.EnumerateArray())
                            {
                                arr[idx++] = c.ValueKind switch
                                {
                                    JsonValueKind.Number => c.TryGetInt64(out var l) ? l : c.GetDouble(),
                                    JsonValueKind.String => c.GetString(),
                                    JsonValueKind.True => true,
                                    JsonValueKind.False => false,
                                    JsonValueKind.Null => null,
                                    _ => c.ToString()
                                };
                            }
                            rows.Add(arr);
                        }
                    }
                }
            }
        }
        catch { }
        return rows;
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task MultiTier_1m_5m_15m_60m_Create_And_Ohlc_Sanity()
    {
        await PhysicalTestEnv.KsqlHelpers.WaitForKsqlReadyAsync("http://127.0.0.1:18088", TimeSpan.FromSeconds(180), graceMs: 2000);
        await using var ctx = new TestContext();

        using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "127.0.0.1:39092" }).Build())
        {
            try { await admin.CreateTopicsAsync(new[] { new TopicSpecification { Name = "deduprates", NumPartitions = 1, ReplicationFactor = 1 } }); } catch { }
            await PhysicalTestEnv.TopicHelpers.WaitForTopicReady(admin, "deduprates", 1, 1, TimeSpan.FromSeconds(10));
        }
        await ctx.WaitForEntityReadyAsync<Rate>(TimeSpan.FromSeconds(60));

        // Phase A: Smoke — push waits (start before produce) for existence of each tier
        var w1m  = QueryStreamCountHttpAsync("SELECT * FROM bar_1m_live  WHERE Broker='B1' AND Symbol='S1' EMIT CHANGES LIMIT 1;", 1, TimeSpan.FromSeconds(180));
        var w5m  = QueryStreamCountHttpAsync("SELECT * FROM bar_5m_live  WHERE Broker='B1' AND Symbol='S1' EMIT CHANGES LIMIT 1;", 1, TimeSpan.FromSeconds(180));
        var w15m = QueryStreamCountHttpAsync("SELECT * FROM bar_15m_live WHERE Broker='B1' AND Symbol='S1' EMIT CHANGES LIMIT 1;", 1, TimeSpan.FromSeconds(180));
        var w60m = QueryStreamCountHttpAsync("SELECT * FROM bar_60m_live WHERE Broker='B1' AND Symbol='S1' EMIT CHANGES LIMIT 1;", 1, TimeSpan.FromSeconds(180));

        // 基準時刻（決定論）: T0 = ceil(now,1m)+5s
        var now = DateTime.UtcNow;
        var ceilMin = new DateTime(now.Ticks - (now.Ticks % TimeSpan.TicksPerMinute), DateTimeKind.Utc).AddMinutes(1);
        var T0 = ceilMin.AddSeconds(5);
        // バケット境界
        DateTime M1  = new DateTime((T0.Ticks / TimeSpan.TicksPerMinute) * TimeSpan.TicksPerMinute, DateTimeKind.Utc);
        DateTime M5  = new DateTime((T0.Ticks / (TimeSpan.TicksPerMinute*5)) * (TimeSpan.TicksPerMinute*5), DateTimeKind.Utc);
        DateTime M15 = new DateTime((T0.Ticks / (TimeSpan.TicksPerMinute*15)) * (TimeSpan.TicksPerMinute*15), DateTimeKind.Utc);
        DateTime W0  = new DateTime((T0.Ticks / (TimeSpan.TicksPerHour)) * (TimeSpan.TicksPerHour), DateTimeKind.Utc);
        DateTime W1  = W0.AddHours(1);

        // Phase B: 厳密OHLC（1m）— [100, 103, 99, 102]
        await ctx.Rates.AddAsync(new Rate { Broker = "B1", Symbol = "S1", Timestamp = M1.AddSeconds( 5), Bid = 100 }); // Open
        await ctx.Rates.AddAsync(new Rate { Broker = "B1", Symbol = "S1", Timestamp = M1.AddSeconds(15), Bid = 103 }); // High
        await ctx.Rates.AddAsync(new Rate { Broker = "B1", Symbol = "S1", Timestamp = M1.AddSeconds(30), Bid =  99 }); // Low
        await ctx.Rates.AddAsync(new Rate { Broker = "B1", Symbol = "S1", Timestamp = M1.AddSeconds(55), Bid = 102 }); // Close

        // Phase B: 厳密OHLC（5m）— M5〜M5+4m に値をばらまき、極値を1つ混ぜる
        // minute 0..4: 101, 120(極高), 95(極低), 104, 103
        var vals5 = new[] { 101d, 120d, 95d, 104d, 103d };
        for (int i = 0; i < 5; i++)
        {
            var ms = M5.AddMinutes(i);
            await ctx.Rates.AddAsync(new Rate { Broker = "B1", Symbol = "S1", Timestamp = ms.AddSeconds(10), Bid = vals5[i] });
        }

        // Phase B: 厳密OHLC（15m）— M15〜M15+14m。始値/終値を明確化
        // open=200, 掃き出しの極高=230（中盤）、極低=190（後半）、close=205
        await ctx.Rates.AddAsync(new Rate { Broker = "B1", Symbol = "S1", Timestamp = M15.AddSeconds( 3), Bid = 200 }); // Open
        await ctx.Rates.AddAsync(new Rate { Broker = "B1", Symbol = "S1", Timestamp = M15.AddMinutes(6).AddSeconds(10), Bid = 230 }); // High
        await ctx.Rates.AddAsync(new Rate { Broker = "B1", Symbol = "S1", Timestamp = M15.AddMinutes(12).AddSeconds(20), Bid = 190 }); // Low
        await ctx.Rates.AddAsync(new Rate { Broker = "B1", Symbol = "S1", Timestamp = M15.AddMinutes(14).AddSeconds(55), Bid = 205 }); // Close

        // Phase C: 厳密OHLC（60m × 2バケット）— W0/W1 に別パターン + 境界近傍の極値で誤配検出
        // W0: 300(open) → 340(high) → 280(low) → 310(close) → [W0終端-1s に 999 (極高)]
        await ctx.Rates.AddAsync(new Rate { Broker = "B1", Symbol = "S1", Timestamp = W0.AddMinutes( 1), Bid = 300 });
        await ctx.Rates.AddAsync(new Rate { Broker = "B1", Symbol = "S1", Timestamp = W0.AddMinutes(10), Bid = 340 });
        await ctx.Rates.AddAsync(new Rate { Broker = "B1", Symbol = "S1", Timestamp = W0.AddMinutes(40), Bid = 280 });
        await ctx.Rates.AddAsync(new Rate { Broker = "B1", Symbol = "S1", Timestamp = W0.AddMinutes(59).AddSeconds(50), Bid = 310 });
        // 境界直前の極高（W0にのみ効くべき）
        await ctx.Rates.AddAsync(new Rate { Broker = "B1", Symbol = "S1", Timestamp = W1.AddSeconds(-1), Bid = 999 });
        // W1: 400(open) → 360(low) → 450(high) → 420(close) → [W1開始+1s に 1 (極低)]
        await ctx.Rates.AddAsync(new Rate { Broker = "B1", Symbol = "S1", Timestamp = W1.AddMinutes( 1), Bid = 400 });
        await ctx.Rates.AddAsync(new Rate { Broker = "B1", Symbol = "S1", Timestamp = W1.AddMinutes( 5), Bid = 360 });
        await ctx.Rates.AddAsync(new Rate { Broker = "B1", Symbol = "S1", Timestamp = W1.AddMinutes(50), Bid = 450 });
        await ctx.Rates.AddAsync(new Rate { Broker = "B1", Symbol = "S1", Timestamp = W1.AddMinutes(59).AddSeconds(50), Bid = 420 });
        // 境界直後の極低（W1にのみ効くべき）
        await ctx.Rates.AddAsync(new Rate { Broker = "B1", Symbol = "S1", Timestamp = W1.AddSeconds(1), Bid = 1 });

        // Await push confirmations
        _ = await w1m; _ = await w5m; _ = await w15m; _ = await w60m;

        // Pull (HTTP) — Phase A の成立確認（各tier >=1）
        async Task<int> CountAsync(string table)
        {
            var rows = await QueryRowsHttpAsync($"SELECT BucketStart, Open, High, Low, KsqlTimeFrameClose FROM {table} WHERE Broker='B1' AND Symbol='S1';", TimeSpan.FromSeconds(20));
            return rows.Count;
        }
        var c1  = await CountAsync("bar_1m_live");
        var c5  = await CountAsync("bar_5m_live");
        var c15 = await CountAsync("bar_15m_live");
        var c60 = await CountAsync("bar_60m_live");
        Assert.True(c1  >= 1, $"bar_1m_live empty");
        Assert.True(c5  >= 1, $"bar_5m_live empty");
        Assert.True(c15 >= 1, $"bar_15m_live empty");
        Assert.True(c60 >= 1, $"bar_60m_live empty");

        // Phase B — 厳密OHLC: 1m/5m/15m バケット検証
        static long Ms(DateTime dt) => (long)(dt - DateTime.UnixEpoch).TotalMilliseconds;
        var bs1m  = Ms(M1);
        var rows1m = await QueryRowsHttpAsync($"SELECT BucketStart, Open, High, Low, KsqlTimeFrameClose FROM bar_1m_live WHERE Broker='B1' AND Symbol='S1' AND BucketStart={bs1m};", TimeSpan.FromSeconds(20));
        if (rows1m.Count > 0)
        {
            var r = rows1m[0];
            var o = Convert.ToDouble(r[1]!);
            var h = Convert.ToDouble(r[2]!);
            var l = Convert.ToDouble(r[3]!);
            var c = Convert.ToDouble(r[4]!);
            Assert.True(o == 100 && h == 103 && l == 99 && c == 102, $"1m OHLC mismatch: O={o},H={h},L={l},C={c}");
            Assert.True(Convert.ToInt64(r[0]!) == bs1m, "1m BucketStart boundary mismatch");
        }

        var bs5m  = Ms(M5);
        var rows5m = await QueryRowsHttpAsync($"SELECT BucketStart, Open, High, Low, KsqlTimeFrameClose FROM bar_5m_live WHERE Broker='B1' AND Symbol='S1' AND BucketStart={bs5m};", TimeSpan.FromSeconds(20));
        if (rows5m.Count > 0)
        {
            var r = rows5m[0];
            var o = Convert.ToDouble(r[1]!);
            var h = Convert.ToDouble(r[2]!);
            var l = Convert.ToDouble(r[3]!);
            var c = Convert.ToDouble(r[4]!);
            Assert.True(h == 120 && l == 95, $"5m High/Low mismatch: H={h},L={l}");
            Assert.True(Convert.ToInt64(r[0]!) == bs5m, "5m BucketStart boundary mismatch");
        }

        var bs15m = Ms(M15);
        var rows15m = await QueryRowsHttpAsync($"SELECT BucketStart, Open, High, Low, KsqlTimeFrameClose FROM bar_15m_live WHERE Broker='B1' AND Symbol='S1' AND BucketStart={bs15m};", TimeSpan.FromSeconds(20));
        if (rows15m.Count > 0)
        {
            var r = rows15m[0];
            var o = Convert.ToDouble(r[1]!);
            var h = Convert.ToDouble(r[2]!);
            var l = Convert.ToDouble(r[3]!);
            var c = Convert.ToDouble(r[4]!);
            Assert.True(o == 200 && h == 230 && l == 190 && c == 205, $"15m OHLC mismatch: O={o},H={h},L={l},C={c}");
            Assert.True(Convert.ToInt64(r[0]!) == bs15m, "15m BucketStart boundary mismatch");
        }

        // Phase C — 60m x 2 バケット検証
        var bsW0 = Ms(W0);
        var rows60_W0 = await QueryRowsHttpAsync($"SELECT BucketStart, Open, High, Low, KsqlTimeFrameClose FROM bar_60m_live WHERE Broker='B1' AND Symbol='S1' AND BucketStart={bsW0};", TimeSpan.FromSeconds(20));
        if (rows60_W0.Count > 0)
        {
            var r = rows60_W0[0];
            var o = Convert.ToDouble(r[1]!);
            var h = Convert.ToDouble(r[2]!);
            var l = Convert.ToDouble(r[3]!);
            var c = Convert.ToDouble(r[4]!);
            Assert.True(o == 300 && h == 999 && l == 280 && c == 310, $"60m W0 OHLC mismatch: O={o},H={h},L={l},C={c}");
            Assert.True(Convert.ToInt64(r[0]!) == bsW0, "60m W0 BucketStart boundary mismatch");
        }
        var bsW1 = Ms(W1);
        var rows60_W1 = await QueryRowsHttpAsync($"SELECT BucketStart, Open, High, Low, KsqlTimeFrameClose FROM bar_60m_live WHERE Broker='B1' AND Symbol='S1' AND BucketStart={bsW1};", TimeSpan.FromSeconds(20));
        if (rows60_W1.Count > 0)
        {
            var r = rows60_W1[0];
            var o = Convert.ToDouble(r[1]!);
            var h = Convert.ToDouble(r[2]!);
            var l = Convert.ToDouble(r[3]!);
            var c = Convert.ToDouble(r[4]!);
            Assert.True(o == 400 && h == 450 && l == 1 && c == 420, $"60m W1 OHLC mismatch: O={o},H={h},L={l},C={c}");
            Assert.True(Convert.ToInt64(r[0]!) == bsW1, "60m W1 BucketStart boundary mismatch");
        }
    }
}
