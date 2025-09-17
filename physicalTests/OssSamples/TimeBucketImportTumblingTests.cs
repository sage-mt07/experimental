using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafka.Ksql.Linq;
using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Core.Modeling;
using Kafka.Ksql.Linq.Query.Dsl;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Integration;

/// <summary>
/// TimeBucket を使用してデータのインポート後、
/// Tumbling を使用したクエリ定義により生成された足データを抽出する物理テスト。
/// 実行にはローカルの Kafka/SchemaRegistry/ksqlDB 環境が必要です。
/// </summary>
public class TimeBucketImportTumblingTests
{
    [KsqlTopic("ticks_tbimp")]
    private class Tick
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlTimestamp] public DateTime TimestampUtc { get; set; }
        public decimal Bid { get; set; }
    }

    [KsqlTopic("bar_tbimp")]
    private class Bar
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlKey(3)] public DateTime BucketStart { get; set; }
        public decimal Open { get; set; }
        public decimal High { get; set; }
        public decimal Low { get; set; }
        public decimal Close { get; set; }
    }

    private sealed class TestContext : KsqlContext
    {
        private static readonly ILoggerFactory _loggerFactory = LoggerFactory.Create(b =>
        {
            b.SetMinimumLevel(LogLevel.Information);
            b.AddFilter("Kafka.Ksql.Linq", LogLevel.Information);
            b.AddConsole();
        });
        public TestContext() : base(new KsqlDslOptions
        {
            // Resolve endpoints from environment when available (Docker runner),
            // fallback to host defaults when running locally
            Common = new CommonSection
            {
                BootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "127.0.0.1:39092"
            },
            SchemaRegistry = new Kafka.Ksql.Linq.Core.Configuration.SchemaRegistrySection
            {
                Url = Environment.GetEnvironmentVariable("SCHEMA_REGISTRY_URL") ?? "http://127.0.0.1:18081"
            },
            KsqlDbUrl = Environment.GetEnvironmentVariable("KSQLDB_URL") ?? "http://127.0.0.1:18088",
            Entities =
            {
                new Kafka.Ksql.Linq.Configuration.EntityConfiguration
                {
                    Entity = nameof(Bar),
                    EnableCache = true
                }
            },
            Topics =
            {
                ["ticks"] = new Kafka.Ksql.Linq.Configuration.Messaging.TopicSection
                {
                    Producer = new Kafka.Ksql.Linq.Configuration.Messaging.ProducerSection
                    {
                        // Ensure immediate send for small test messages
                        Acks = "All",
                        EnableIdempotence = true,
                        MaxInFlightRequestsPerConnection = 1,
                        LingerMs = 0,
                        BatchNumMessages = 1,
                        DeliveryTimeoutMs = 30000,
                        RetryBackoffMs = 100,
                        // Optional low-latency tweaks
                        AdditionalProperties = new System.Collections.Generic.Dictionary<string, string>
                        {
                            ["socket.keepalive.enable"] = "true",
                            ["queue.buffering.max.ms"] = "0"
                        }
                    }
                }
            }
        }, _loggerFactory) { }

        public EventSet<Tick> Ticks { get; set; } = null!;

        protected override void OnModelCreating(IModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Bar>()
                .ToQuery(q => q.From<Tick>()
                    .Tumbling(r => r.TimestampUtc, new Windows { Minutes = new[] { 1, 5 } })
                    .GroupBy(r => new { r.Broker, r.Symbol })
                    .Select(g => new Bar
                    {
                        Broker = g.Key.Broker,
                        Symbol = g.Key.Symbol,
                        BucketStart = g.WindowStart(),
                        Open = g.EarliestByOffset(x => x.Bid),
                        High = g.Max(x => x.Bid),
                        Low = g.Min(x => x.Bid),
                        Close = g.LatestByOffset(x => x.Bid)
                    })
                );
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task Import_Ticks_Define_Tumbling_Query_Then_Extract_Bars_Via_TimeBucket()
    {
        var queryTimeoutSeconds = Environment.GetEnvironmentVariable("KSQL_QUERY_RUNNING_TIMEOUT_SECONDS");
        var timeout = TimeSpan.FromSeconds(
            int.TryParse(queryTimeoutSeconds, out var seconds) && seconds > 0 ? seconds : 180);

        // Local RocksDB state may affect table materialization; clear it first
        try { PhysicalTestEnv.Cleanup.DeleteLocalRocksDbState(); } catch { }
        // Ensure env is ready (honor Docker runner env if set)
        var brokers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "127.0.0.1:39092";
        var srUrl = Environment.GetEnvironmentVariable("SCHEMA_REGISTRY_URL") ?? "http://127.0.0.1:18081";
        var ksqlUrl = Environment.GetEnvironmentVariable("KSQLDB_URL") ?? "http://127.0.0.1:18088";

        await PhysicalTestEnv.Health.WaitForKafkaAsync(brokers, timeout);
        await PhysicalTestEnv.Health.WaitForHttpOkAsync(srUrl.TrimEnd('/') + "/subjects", timeout);
        await PhysicalTestEnv.KsqlHelpers.WaitForKsqlReadyAsync(ksqlUrl, timeout, graceMs: 3000);
        // Drop any previously created artifacts to avoid conflicts
        try { await PhysicalTestEnv.KsqlHelpers.TerminateAndDropBarArtifactsAsync(ksqlUrl); } catch { }

        // Pre-create source and DLQ topics
        using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = brokers }).Build())
        {
            try { await admin.CreateTopicsAsync(new[] { new TopicSpecification { Name = "ticks_tbimp", NumPartitions = 1, ReplicationFactor = 1 } }); } catch { }
            await PhysicalTestEnv.TopicHelpers.WaitForTopicReady(admin, "ticks_tbimp", 1, 1, TimeSpan.FromSeconds(60));
            try { await admin.CreateTopicsAsync(new[] { new TopicSpecification { Name = "dead-letter-queue", NumPartitions = 1, ReplicationFactor = 1 } }); } catch { }
            await PhysicalTestEnv.TopicHelpers.WaitForTopicReady(admin, "dead-letter-queue", 1, 1, TimeSpan.FromSeconds(60));
        }
        // Create context (may issue schema registration and DDLs)
        using var ctx = new TestContext();

        var broker = "B"; var symbol = "S";
        var baseTime = DateTime.UtcNow.AddMinutes(-8);

        // Kick off push queries (Emit Changes) in separate tasks to ensure materialization
        async Task<int> QueryStreamCountHttpAsync(string table, int limit, TimeSpan timeout)
        {
            using var http = new HttpClient { BaseAddress = new Uri(ksqlUrl) };
            var sql = $"SELECT * FROM {table} EMIT CHANGES LIMIT {limit};";
            var payload = new { sql };
            var json = System.Text.Json.JsonSerializer.Serialize(payload);
            using var req = new HttpRequestMessage(HttpMethod.Post, "/query-stream")
            {
                Content = new StringContent(json, System.Text.Encoding.UTF8, "application/json")
            };
            using var cts = new CancellationTokenSource(timeout);
            using var resp = await http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, cts.Token);
            resp.EnsureSuccessStatusCode();
            await using var stream = await resp.Content.ReadAsStreamAsync(cts.Token);
            using var reader = new System.IO.StreamReader(stream, System.Text.Encoding.UTF8);
            int count = 0;
            var deadlineStream = DateTime.UtcNow + timeout;
            while (!cts.IsCancellationRequested)
            {
                var remaining = deadlineStream - DateTime.UtcNow;
                if (remaining <= TimeSpan.Zero) break;

                var readTask = reader.ReadLineAsync();
                var delayTask = Task.Delay(TimeSpan.FromMilliseconds(Math.Min(1000, Math.Max(1, remaining.TotalMilliseconds))), cts.Token);
                var completed = await Task.WhenAny(readTask, delayTask);
                if (completed != readTask)
                {
                    // timed out waiting for a line
                    break;
                }
                var line = await readTask;
                if (line == null) break;
                if (line.IndexOf("\"row\"", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    count++;
                    if (count >= limit) break;
                }
            }
            return count;
        }

        var live1m = "bar_tbimp_1m_live";
        var live5m = "bar_tbimp_5m_live";
        var wait1mTask = QueryStreamCountHttpAsync(live1m, 1, timeout);
        var wait5mTask = QueryStreamCountHttpAsync(live5m, 1, timeout);

        // Import a continuous sequence of ticks (~2 minutes) to ensure materialization
        var bids = new decimal[] { 100m, 110m, 95m, 105m, 120m, 115m, 112m, 118m };
        for (int i = 0; i < 120; i++)
        {
            var ts = baseTime.AddSeconds(i);
            var bid = bids[i % bids.Length];
            await ctx.Ticks.AddAsync(new Tick { Broker = broker, Symbol = symbol, TimestampUtc = ts, Bid = bid });
            await Task.Delay(5);
        }

        // Ensure derived tables exist before querying
        await WaitTablesReadyAsync(new Uri(ksqlUrl), timeout);
        // Ensure push observers saw rows (materialization complete) before fetching via TimeBucket
        _ = await wait1mTask; _ = await wait5mTask;

        // Use static TimeBucket API with KsqlContext
        var bars1m = await Kafka.Ksql.Linq.TimeBucket.Get<Bar>(ctx, Kafka.Ksql.Linq.Runtime.Period.Minutes(1))
            .ToListAsync(new[] { broker, symbol }, CancellationToken.None);
        var bars5m = await Kafka.Ksql.Linq.TimeBucket.Get<Bar>(ctx, Kafka.Ksql.Linq.Runtime.Period.Minutes(5))
            .ToListAsync(new[] { broker, symbol }, CancellationToken.None);
        Assert.True(bars1m.Count >= 1, "1m TimeBucket returned no rows");
        Assert.True(bars5m.Count >= 1, "5m TimeBucket returned no rows");

        // Basic shape checks
        var first = bars1m.OrderBy(b => b.BucketStart).First();
        Assert.Equal(broker, first.Broker);
        Assert.Equal(symbol, first.Symbol);
    }

    // Removed TimeBucket-based wait; use direct Pull queries above

    private static async Task<System.Collections.Generic.List<object?[]>> QueryRowsAsync(string sql, Uri baseUrl, TimeSpan timeout)
    {
        using var http = new HttpClient { BaseAddress = baseUrl };
        // ksqlDB REST: /query expects body key "sql" (not "ksql").
        var payload = new { sql = sql, properties = new System.Collections.Generic.Dictionary<string, object>() };
        using var content = new StringContent(System.Text.Json.JsonSerializer.Serialize(payload), System.Text.Encoding.UTF8, "application/json");
        using var cts = new CancellationTokenSource(timeout);
        using var resp = await http.PostAsync("/query", content, cts.Token);
        var rows = new System.Collections.Generic.List<object?[]>();
        if (resp.IsSuccessStatusCode)
        {
            var body = await resp.Content.ReadAsStringAsync(cts.Token);
            try
            {
                using var doc = System.Text.Json.JsonDocument.Parse(body);
                if (doc.RootElement.ValueKind == System.Text.Json.JsonValueKind.Array)
                {
                    foreach (var el in doc.RootElement.EnumerateArray())
                    {
                        if (el.ValueKind == System.Text.Json.JsonValueKind.Object && el.TryGetProperty("row", out var rowEl))
                        {
                            if (rowEl.TryGetProperty("columns", out var cols) && cols.ValueKind == System.Text.Json.JsonValueKind.Array)
                            {
                                var arr = new object?[cols.GetArrayLength()];
                                int idx = 0;
                                foreach (var c in cols.EnumerateArray())
                                {
                                    arr[idx++] = c.ValueKind switch
                                    {
                                        System.Text.Json.JsonValueKind.Number => c.TryGetInt64(out var l) ? l : c.GetDouble(),
                                        System.Text.Json.JsonValueKind.String => c.GetString(),
                                        System.Text.Json.JsonValueKind.True => true,
                                        System.Text.Json.JsonValueKind.False => false,
                                        System.Text.Json.JsonValueKind.Null => null,
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
        // Fallback to push query (streaming) when pull fails
        var sqlPush = sql.Trim();
        if (sqlPush.EndsWith(";", StringComparison.Ordinal)) sqlPush = sqlPush[..^1];
        // remove existing LIMIT clause if present to avoid double LIMIT
        sqlPush = System.Text.RegularExpressions.Regex.Replace(sqlPush, @"\s+LIMIT\s+\d+\s*$", string.Empty, System.Text.RegularExpressions.RegexOptions.IgnoreCase);
        sqlPush += " EMIT CHANGES LIMIT 10;";
        // ksqlDB REST: /query-stream expects body key "sql" (not "ksql").
        var payload2 = new { sql = sqlPush, properties = new System.Collections.Generic.Dictionary<string, object>() };
        using var content2 = new StringContent(System.Text.Json.JsonSerializer.Serialize(payload2), System.Text.Encoding.UTF8, "application/json");
        using var req = new HttpRequestMessage(HttpMethod.Post, "/query-stream") { Content = content2 };
        using var resp2 = await http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, cts.Token);
        resp2.EnsureSuccessStatusCode();
        await using var stream = await resp2.Content.ReadAsStreamAsync(cts.Token);
        using var reader = new System.IO.StreamReader(stream, System.Text.Encoding.UTF8);
        var deadline = DateTime.UtcNow + timeout;
        while (!cts.IsCancellationRequested)
        {
            var remaining = deadline - DateTime.UtcNow;
            if (remaining <= TimeSpan.Zero) break;

            var readTask = reader.ReadLineAsync();
            var delayTask = Task.Delay(TimeSpan.FromMilliseconds(Math.Min(1000, Math.Max(1, remaining.TotalMilliseconds))), cts.Token);
            var completed = await Task.WhenAny(readTask, delayTask);
            if (completed != readTask)
            {
                // timed out waiting for a line within the remaining budget
                break;
            }

            var line = await readTask;
            if (string.IsNullOrWhiteSpace(line)) continue;
            if (line.IndexOf("\"row\"", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                try
                {
                    using var j = System.Text.Json.JsonDocument.Parse(line);
                    if (j.RootElement.TryGetProperty("row", out var rEl) && rEl.TryGetProperty("columns", out var cols) && cols.ValueKind == System.Text.Json.JsonValueKind.Array)
                    {
                        var arr = new object?[cols.GetArrayLength()];
                        int idx = 0;
                        foreach (var c in cols.EnumerateArray())
                        {
                            arr[idx++] = c.ValueKind switch
                            {
                                System.Text.Json.JsonValueKind.Number => c.TryGetInt64(out var l) ? l : c.GetDouble(),
                                System.Text.Json.JsonValueKind.String => c.GetString(),
                                System.Text.Json.JsonValueKind.True => true,
                                System.Text.Json.JsonValueKind.False => false,
                                System.Text.Json.JsonValueKind.Null => null,
                                _ => c.ToString()
                            };
                        }
                        rows.Add(arr);
                        if (rows.Count >= 1) break;
                    }
                }
                catch { }
            }
        }
        return rows;
    }

    private static async Task WaitTablesReadyAsync(Uri baseUrl, TimeSpan timeout)
    {
        using var http = new HttpClient { BaseAddress = baseUrl };
        var deadline = DateTime.UtcNow + timeout;
        var names = new[] { "BAR_TBIMP_1M_LIVE", "BAR_TBIMP_5M_LIVE" };
        while (DateTime.UtcNow < deadline)
        {
            var allOk = true;
            foreach (var name in names)
            {
                var payload = new { ksql = $"DESCRIBE {name};", streamsProperties = new System.Collections.Generic.Dictionary<string, object>() };
                using var content = new StringContent(System.Text.Json.JsonSerializer.Serialize(payload), System.Text.Encoding.UTF8, "application/json");
                try
                {
                    using var resp = await http.PostAsync("/ksql", content);
                    var body = await resp.Content.ReadAsStringAsync();
                    if (!resp.IsSuccessStatusCode || body.IndexOf("\"statement_error\"", StringComparison.OrdinalIgnoreCase) >= 0)
                    { allOk = false; break; }
                }
                catch { allOk = false; break; }
            }
            if (allOk) return;
            await Task.Delay(1000);
        }
        throw new TimeoutException("bar_tbimp_1m_live/bar_tbimp_5m_live not ready within timeout");
    }
    
}

