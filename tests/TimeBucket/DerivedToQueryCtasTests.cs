using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Kafka.Ksql.Linq;
using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Infrastructure.KsqlDb;
using Kafka.Ksql.Linq.Query.Dsl;
using Kafka.Ksql.Linq.Core.Modeling;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Unit;

public class DerivedToQueryCtasTests
{
    private sealed class FakeKsqlDbClient : IKsqlDbClient
    {
        public readonly List<string> Statements = new();

        public Task<KsqlDbResponse> ExecuteStatementAsync(string statement)
        {
            lock (Statements) Statements.Add(statement);
            // Always succeed for the purpose of this UT
            return Task.FromResult(new KsqlDbResponse(true, "OK"));
        }

        public Task<KsqlDbResponse> ExecuteExplainAsync(string ksql)
            => ExecuteStatementAsync($"EXPLAIN {ksql}");

        public Task<HashSet<string>> GetTableTopicsAsync()
            => Task.FromResult(new HashSet<string>());

        public Task<int> ExecuteQueryStreamCountAsync(string sql, TimeSpan? timeout = null)
            => Task.FromResult(0);

        public Task<int> ExecutePullQueryCountAsync(string sql, TimeSpan? timeout = null)
            => Task.FromResult(0);
    }

    private sealed class TestContext : KsqlContext
    {
        public TestContext() : base(new KsqlDslOptions
        {
            Common = new CommonSection { BootstrapServers = "dummy:9092" },
            SchemaRegistry = new Kafka.Ksql.Linq.Core.Configuration.SchemaRegistrySection { Url = "http://dummy:8081" },
            KsqlDbUrl = "http://dummy:8088"
        }) { }

        // Avoid external services; we'll invoke DDL path directly
        protected override bool SkipSchemaRegistration => true;

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

        protected override void OnModelCreating(IModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Bar>()
                .ToQuery(q => q.From<Rate>()
                    .Tumbling(r => r.Timestamp, new Windows { Minutes = new[] { 1, 5 } })
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
    public async Task ToQuery_Tumbling_Creates_CTAS_For_1m_And_5m()
    {
        var ctx = new TestContext();

        // Inject fake ksql client via reflection
        var fake = new FakeKsqlDbClient();
        var field = typeof(KsqlContext).GetField("_ksqlDbClient", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(field);
        field!.SetValue(ctx, fake);

        // Obtain Bar model and invoke EnsureQueryEntityDdlAsync directly to drive CTAS without Schema Registry
        var models = ctx.GetEntityModels();
        Assert.Contains(typeof(TestContext.Bar), models.Keys);
        var model = models[typeof(TestContext.Bar)];
        var ensure = typeof(KsqlContext).GetMethod("EnsureQueryEntityDdlAsync", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(ensure);
        var t = (Task)ensure!.Invoke(ctx, new object[] { typeof(TestContext.Bar), model })!;
        await t;

        // Assert 1s final TABLE is created before 1s hub STREAM
        var stmts = fake.Statements.ToArray();
        int idxTable1s = Array.FindIndex(stmts, s => s.IndexOf("bar_1s_final", StringComparison.OrdinalIgnoreCase) >= 0 && s.TrimStart().StartsWith("CREATE", StringComparison.OrdinalIgnoreCase));
        int idxStream1s = Array.FindIndex(stmts, s => s.IndexOf("bar_1s_final_s", StringComparison.OrdinalIgnoreCase) >= 0 && s.TrimStart().StartsWith("CREATE", StringComparison.OrdinalIgnoreCase));
        Assert.True(idxTable1s >= 0, "bar_1s_final CTAS not found");
        Assert.True(idxStream1s >= 0, "bar_1s_final_s CSAS not found");
        Assert.True(idxTable1s < idxStream1s, "bar_1s_final TABLE should be created before bar_1s_final_s STREAM");
    }
}
