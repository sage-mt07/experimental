using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq;
using Kafka.Ksql.Linq.Query.Analysis;
using Kafka.Ksql.Linq.Query.Dsl;
using Kafka.Ksql.Linq.Mapping;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading.Tasks;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Analysis;

[KsqlTopic("test-topic")]
class ConcurrencySource
{
    public int Id { get; set; }
}

public class DerivedTumblingPipelineConcurrencyTests
{
    [Fact]
    public async Task RunAsync_registers_all_models_without_conflict()
    {
        var qao = new TumblingQao
        {
            TimeKey = "Timestamp",
            Windows = new[] { new Timeframe(1, "m"), new Timeframe(5, "m") },
            Keys = new[] { "Id", "BucketStart" },
            Projection = new[] { "Id", "BucketStart", "KsqlTimeFrameClose" },
            PocoShape = new[]
            {
                new ColumnShape("Id", typeof(int), false),
                new ColumnShape("BucketStart", typeof(long), false),
                new ColumnShape("KsqlTimeFrameClose", typeof(double), false)
            },
            BasedOn = new BasedOnSpec(new[] { "Id", "BucketStart" }, string.Empty, "KsqlTimeFrameClose", string.Empty),
            WeekAnchor = DayOfWeek.Monday
        };
        var baseModel = new EntityModel { EntityType = typeof(ConcurrencySource) };
        var model = new KsqlQueryModel
        {
            SourceTypes = new[] { typeof(ConcurrencySource) },
            Windows = { "1m", "5m" }
        };
        var ddls = new ConcurrentBag<string>();
        Task<KsqlDbResponse> Exec(EntityModel _, string sql)
        {
            ddls.Add(sql);
            return Task.FromResult(new KsqlDbResponse(true, string.Empty));
        }
        var mapping = new MappingRegistry();
        var registry = new ConcurrentDictionary<Type, EntityModel>();
        var asm = AssemblyBuilder.DefineDynamicAssembly(new AssemblyName("dyn"), AssemblyBuilderAccess.Run);
        var mod = asm.DefineDynamicModule("m");
        Type Resolver(string _) => mod.DefineType("T" + Guid.NewGuid().ToString("N")).CreateType()!;

        // Enable WhenEmpty to include HB per timeframe
        model.WhenEmptyFiller = (System.Linq.Expressions.Expression<System.Func<int,int,int>>)((a,b) => a);
        _ = await DerivedTumblingPipeline.RunAsync(qao, baseModel, model, Exec, Resolver, mapping, registry, new LoggerFactory().CreateLogger("test"));

        // WhenEmpty 有効化により、HB/Fill/Prev が追加されうるため件数は増加する。
        // 現仕様では: 1s (stream, final, hb) + 1m(live, hb, prev, fill) + 5m(live, hb, fill) = 10
        var expected = 10;
        Assert.Equal(expected, registry.Count);
        Assert.Equal(expected, ddls.Count);
        var finals = ddls.Where(d => d.Contains("_final") && !d.Contains("_final_s")).ToList();
        Assert.Single(finals);
        Assert.Contains("EMIT FINAL", finals[0]);
    }
}

