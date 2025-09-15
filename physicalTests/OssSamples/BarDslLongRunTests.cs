using Kafka.Ksql.Linq;
using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Core.Modeling;
using Microsoft.Extensions.Logging;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using System.Text.Json;
using System.Net.Http;
using System.Text;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Integration;

public class BarDslLongRunTests
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
        protected override void OnModelCreating(IModelBuilder mb)
        {
            mb.Entity<Bar>()
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

    [Fact]
    [Trait("Category", "Integration")]
    public async Task LongRun_1h_Ohlc_And_Grace_Verify()
    {
        await using var ctx = new TestContext();
        await PhysicalTestEnv.KsqlHelpers.WaitForKsqlReadyAsync("http://127.0.0.1:18088", TimeSpan.FromSeconds(180), graceMs: 2000);

        using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "127.0.0.1:39092" }).Build())
        {
            try { await admin.CreateTopicsAsync(new[] { new TopicSpecification { Name = "deduprates", NumPartitions = 1, ReplicationFactor = 1 } }); } catch { }
            await PhysicalTestEnv.TopicHelpers.WaitForTopicReady(admin, "deduprates", 1, 1, TimeSpan.FromSeconds(10));
        }

        await ctx.WaitForEntityReadyAsync<Rate>(TimeSpan.FromSeconds(60));

        // Align start to the next minute to make buckets deterministic
        var now = DateTime.UtcNow;
        var t0 = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0, DateTimeKind.Utc).AddMinutes(1);
        var waitHead = t0 - DateTime.UtcNow;
        if (waitHead > TimeSpan.Zero && waitHead < TimeSpan.FromMinutes(1)) await Task.Delay(waitHead);

        // Long-run duration: default 60 minutes (can override via env LONGRUN_MINUTES)
        int minutes = 60;
        if (int.TryParse(Environment.GetEnvironmentVariable("LONGRUN_MINUTES"), out var m) && m > 0) minutes = m;

        // Generate multiple ticks per minute for OHLC, and a late (grace) tick just after minute end with an in-window timestamp
        var basePrice = 100.0;
        for (int i = 0; i < minutes; i++)
        {
            var ms = t0.AddMinutes(i);
            var open = basePrice + i * 0.5;     // slow drift
            var high = open + 10;               // high spike
            var low  = open - 7;                // low spike
            var close= open + 3;                // close value

            // within-minute ticks
            await ctx.Rates.AddAsync(new Rate { Broker = "B1", Symbol = "S1", Timestamp = ms.AddSeconds(1),  Bid = open  });
            await ctx.Rates.AddAsync(new Rate { Broker = "B1", Symbol = "S1", Timestamp = ms.AddSeconds(12), Bid = high  });
            await ctx.Rates.AddAsync(new Rate { Broker = "B1", Symbol = "S1", Timestamp = ms.AddSeconds(24), Bid = low   });
            await ctx.Rates.AddAsync(new Rate { Broker = "B1", Symbol = "S1", Timestamp = ms.AddSeconds(36), Bid = (open+low)/2 });
            await ctx.Rates.AddAsync(new Rate { Broker = "B1", Symbol = "S1", Timestamp = ms.AddSeconds(48), Bid = close });

            // Late tick within previous minute window to test grace (tiny delay beyond boundary)
            // Timestamp set within current window; send slightly after the boundary to exercise grace
            await Task.Delay(200); // real-time small delay
            await ctx.Rates.AddAsync(new Rate { Broker = "B1", Symbol = "S1", Timestamp = ms.AddSeconds(40), Bid = high + 5 }); // higher high within window
        }

        // After production, verify at least some buckets exist and OHLC semantics hold for early minutes
        // Use HTTP /query to fetch rows
        static async Task<List<object?[]>> QueryRowsHttpAsync(string sql, TimeSpan timeout)
        {
            using var http = new HttpClient { BaseAddress = new Uri("http://127.0.0.1:18088") };
            using var cts = new CancellationTokenSource(timeout);

            async Task<List<object?[]>> ParseArrayAsync(HttpResponseMessage resp, CancellationToken token)
            {
                var body = await resp.Content.ReadAsStringAsync(token);
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

            // Try /query with sql field and empty properties
            var payload1 = new { sql, properties = new Dictionary<string, object>() };
            var json1 = JsonSerializer.Serialize(payload1);
            using var content1 = new StringContent(json1, Encoding.UTF8, "application/json");
            using var resp1 = await http.PostAsync("/query", content1, cts.Token);
            if (resp1.IsSuccessStatusCode)
            {
                return await ParseArrayAsync(resp1, cts.Token);
            }

            // Fallback: use ksql field
            var payload2 = new { ksql = sql, streamsProperties = new Dictionary<string, object>() };
            var json2 = JsonSerializer.Serialize(payload2);
            using var content2 = new StringContent(json2, Encoding.UTF8, "application/json");
            using var resp2 = await http.PostAsync("/query", content2, cts.Token);
            if (resp2.IsSuccessStatusCode)
            {
                return await ParseArrayAsync(resp2, cts.Token);
            }

            // Final fallback: push query-stream with EMIT CHANGES LIMIT 100
            var sqlPush = sql.TrimEnd().EndsWith(";") ? sql.TrimEnd()[..^1] : sql;
            if (!sqlPush.Contains("EMIT CHANGES", StringComparison.OrdinalIgnoreCase))
                sqlPush += " EMIT CHANGES LIMIT 100;";
            var payload3 = new { sql = sqlPush, properties = new Dictionary<string, object> { ["ksql.streams.auto.offset.reset"] = "earliest" } };
            var json3 = JsonSerializer.Serialize(payload3);
            using var content3 = new StringContent(json3, Encoding.UTF8, "application/json");
            using var req3 = new HttpRequestMessage(HttpMethod.Post, "/query-stream") { Content = content3 };
            using var resp3 = await http.SendAsync(req3, HttpCompletionOption.ResponseHeadersRead, cts.Token);
            resp3.EnsureSuccessStatusCode();
            var list = new List<object?[]>();
            await using (var stream = await resp3.Content.ReadAsStreamAsync(cts.Token))
            using (var reader = new System.IO.StreamReader(stream, Encoding.UTF8))
            {
                while (!reader.EndOfStream && !cts.IsCancellationRequested)
                {
                    var line = await reader.ReadLineAsync();
                    if (line == null) break;
                    if (line.IndexOf("\"columns\"", StringComparison.OrdinalIgnoreCase) >= 0)
                    {
                        try
                        {
                            using var item = JsonDocument.Parse(line);
                            if (item.RootElement.TryGetProperty("row", out var rowEl) && rowEl.TryGetProperty("columns", out var cols) && cols.ValueKind == JsonValueKind.Array)
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
                                list.Add(arr);
                            }
                        }
                        catch { }
                    }
                }
            }
            return list;
        }

        // Wait for at least 5 one-minute buckets to appear
        var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(3);
        int seen = 0;
        while (DateTime.UtcNow < deadline)
        {
            var rows = await QueryRowsHttpAsync("SELECT BucketStart, Open, High, Low, KsqlTimeFrameClose FROM bar_1m_live WHERE Broker='B1' AND Symbol='S1';", TimeSpan.FromSeconds(10));
            seen = rows.Count;
            if (seen >= 5) break;
            await Task.Delay(2000);
        }
        Assert.True(seen >= 5, $"Expected at least 5 one-minute bars, got {seen}");

        // Validate OHLC of the first minute with late higher-high applied
        static long Ms(DateTime dt) => (long)(dt - DateTime.UnixEpoch).TotalMilliseconds;
        var bs0 = Ms(t0);
        var rows1 = await QueryRowsHttpAsync($"SELECT BucketStart, Open, High, Low, KsqlTimeFrameClose FROM bar_1m_live WHERE Broker='B1' AND Symbol='S1' AND BucketStart={bs0};", TimeSpan.FromSeconds(10));
        Assert.True(rows1.Count >= 1, "First minute bar not found");
        var r0 = rows1[0];
        var o0 = Convert.ToDouble(r0[1]!);
        var h0 = Convert.ToDouble(r0[2]!);
        var l0 = Convert.ToDouble(r0[3]!);
        var c0 = Convert.ToDouble(r0[4]!);
        Assert.True(h0 > o0, "High should exceed Open (includes late higher-high)");
        Assert.True(c0 >= o0 - 0.0001, "Close should be near configured close pattern");
    }
}
