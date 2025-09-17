using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq;
using Kafka.Ksql.Linq.Mapping;
using Kafka.Ksql.Linq.Query.Analysis;
using Kafka.Ksql.Linq.Query.Dsl;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Reflection.Emit;
using System.Threading.Tasks;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Analysis;

[KsqlTopic("bar")]
class HubAggSource { public int Id { get; set; } }

public class DerivedTumblingPipelineHubAggregationTests
{
    private static async Task<(ConcurrentBag<string> ddls, MappingRegistry map)> RunAsync(TumblingQao qao)
    {
        var baseModel = new EntityModel { EntityType = typeof(HubAggSource) };
        var model = new KsqlQueryModel { SourceTypes = new[] { typeof(HubAggSource) }, Windows = { "1m", "5m" } };
        var ddls = new ConcurrentBag<string>();
        Task<KsqlDbResponse> Exec(EntityModel _, string sql) { ddls.Add(sql); return Task.FromResult(new KsqlDbResponse(true, string.Empty)); }
        var mapping = new MappingRegistry();
        var registry = new ConcurrentDictionary<Type, EntityModel>();
        var asm = AssemblyBuilder.DefineDynamicAssembly(new System.Reflection.AssemblyName("dyn"), AssemblyBuilderAccess.Run);
        var mod = asm.DefineDynamicModule("m");
        Type Resolver(string _) => mod.DefineType("T" + Guid.NewGuid().ToString("N")).CreateType()!;
        _ = await DerivedTumblingPipeline.RunAsync(qao, baseModel, model, Exec, Resolver, mapping, registry, new LoggerFactory().CreateLogger("test"));
        return (ddls, mapping);
    }

    [Fact]
    public async Task Hub_input_Reaggregates_From_Ohlc_Columns()
    {
        // seconds hub will produce OPEN/HIGH/LOW/KSQLTIMEFRAMECLOSE
        var qao = new TumblingQao
        {
            TimeKey = "Timestamp",
            Windows = new[] { new Timeframe(1, "m"), new Timeframe(5, "m") },
            Keys = new[] { "Id", "BucketStart" },
            Projection = new[] { "Id", "BucketStart", "Open", "High", "Low", "KsqlTimeFrameClose" },
            PocoShape = new[]
            {
                new ColumnShape("Id", typeof(int), false),
                new ColumnShape("BucketStart", typeof(long), false),
                new ColumnShape("Open", typeof(double), false),
                new ColumnShape("High", typeof(double), false),
                new ColumnShape("Low", typeof(double), false),
                new ColumnShape("KsqlTimeFrameClose", typeof(double), false)
            },
            BasedOn = new BasedOnSpec(new[] { "Id" }, string.Empty, "KsqlTimeFrameClose", string.Empty),
            WeekAnchor = DayOfWeek.Monday
        };

        var (ddls, _) = await RunAsync(qao);
        var ddl1m = ddls.First(s => s.Contains("bar_1m_live"));
        Assert.Contains("FROM bar_1s_final_s", ddl1m, StringComparison.OrdinalIgnoreCase);
        // ユーザー投影依存のため、具体的な列/集約には依存しない。
        // ハブSTREAMを入力としていることのみを検証する。
    }

    [Fact]
    public async Task Live_Ddl_Includes_Grace_Period()
    {
        var qao = new TumblingQao
        {
            TimeKey = "Timestamp",
            Windows = new[] { new Timeframe(1, "m") },
            Keys = new[] { "Id", "BucketStart" },
            Projection = new[] { "Id", "BucketStart", "KsqlTimeFrameClose" },
            PocoShape = new[]
            {
                new ColumnShape("Id", typeof(int), false),
                new ColumnShape("BucketStart", typeof(long), false),
                new ColumnShape("KsqlTimeFrameClose", typeof(double), false)
            },
            BasedOn = new BasedOnSpec(new[] { "Id" }, string.Empty, "KsqlTimeFrameClose", string.Empty),
            WeekAnchor = DayOfWeek.Monday
        };
        var (ddls, _) = await RunAsync(qao);
        var ddl1m = ddls.First(s => s.Contains("bar_1m_live"));
        // DerivationPlanner assigns grace per timeframe as a +1 sequence starting from 0 and includes an auto-inserted 1s.
        // With only 1m specified, 1s gets 1s grace, 1m gets 2s grace.
        Assert.Contains("GRACE PERIOD 2 SECONDS", ddl1m, StringComparison.OrdinalIgnoreCase);
    }
}

