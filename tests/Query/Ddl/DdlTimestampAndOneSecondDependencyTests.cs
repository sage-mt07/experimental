using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq;
using Kafka.Ksql.Linq.Mapping;
using Kafka.Ksql.Linq.Query.Analysis;
using Kafka.Ksql.Linq.Query.Ddl;
using Kafka.Ksql.Linq.Query.Pipeline;
using Kafka.Ksql.Linq.Query.Abstractions;
using Kafka.Ksql.Linq.Query.Dsl;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Reflection.Emit;
using System.Threading.Tasks;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Ddl;

public class DdlTimestampAndOneSecondDependencyTests
{
    private class RateTs
    {
        [KsqlTimestamp] public DateTime Timestamp { get; set; }
        public double Bid { get; set; }
    }

    [KsqlTopic("bar")]
    private class BaseEntity { public int Id { get; set; } public long BucketStart { get; set; } }

    [Fact]
    public void GenerateCreateStream_Includes_Timestamp_In_WithClause()
    {
        var model = new EntityModel { EntityType = typeof(RateTs) };
        model.SetStreamTableType(StreamTableType.Stream);
        var adapter = new EntityModelDdlAdapter(model);
        var gen = new DDLQueryGenerator();
        string ddl;
        using (Kafka.Ksql.Linq.Core.Modeling.ModelCreatingScope.Enter())
        {
            ddl = gen.GenerateCreateStream(adapter);
        }
        Assert.Contains("CREATE OR REPLACE STREAM", ddl, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("KAFKA_TOPIC='ratets'", ddl, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("VALUE_FORMAT='AVRO'", ddl, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task OneSecond_TableFromSource_Then_StreamDefinition()
    {
        var qao = new TumblingQao
        {
            TimeKey = "Timestamp",
            Windows = new[] { new Timeframe(1, "m") },
            Keys = new[] { "Id", "BucketStart" },
            Projection = new[] { "Id", "BucketStart" },
            PocoShape = new[]
            {
                new ColumnShape("Id", typeof(int), false),
                new ColumnShape("BucketStart", typeof(long), false)
            },
            BasedOn = new BasedOnSpec(new[] { "Id" }, string.Empty, "BucketStart", string.Empty),
            WeekAnchor = DayOfWeek.Monday
        };
        var baseModel = new EntityModel { EntityType = typeof(BaseEntity) };
        var model = new KsqlQueryModel { SourceTypes = new[] { typeof(BaseEntity) }, Windows = { "1m" } };
        var ddls = new ConcurrentBag<string>();
        Task<KsqlDbResponse> Exec(EntityModel _, string sql) { ddls.Add(sql); return Task.FromResult(new KsqlDbResponse(true, string.Empty)); }
        var mapping = new MappingRegistry();
        var registry = new ConcurrentDictionary<Type, EntityModel>();
        var asm = AssemblyBuilder.DefineDynamicAssembly(new System.Reflection.AssemblyName("dyn"), AssemblyBuilderAccess.Run);
        var mod = asm.DefineDynamicModule("m");
        Type Resolver(string _) => mod.DefineType("T" + Guid.NewGuid().ToString("N")).CreateType()!;
        _ = await DerivedTumblingPipeline.RunAsync(qao, baseModel, model, Exec, Resolver, mapping, registry, new LoggerFactory().CreateLogger("test"));

        var tbl = ddls.First(s => s.StartsWith("CREATE TABLE bar_1s_final", StringComparison.OrdinalIgnoreCase));
        var str = ddls.First(s => s.StartsWith("CREATE STREAM bar_1s_final_s", StringComparison.OrdinalIgnoreCase));
        Assert.Contains("FROM BAR O", tbl, StringComparison.OrdinalIgnoreCase); // source is base stream, not hub
        Assert.Contains("EMIT FINAL", tbl, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("AS SELECT", str, StringComparison.OrdinalIgnoreCase); // definition, not CSAS
        Assert.Contains("KEY_FORMAT='AVRO'", str, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("VALUE_AVRO_SCHEMA_FULL_NAME", str, StringComparison.OrdinalIgnoreCase);
    }
}

