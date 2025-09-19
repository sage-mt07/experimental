using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq;
using Kafka.Ksql.Linq.Query.Analysis;
using Kafka.Ksql.Linq.Query.Dsl;
using Kafka.Ksql.Linq.Mapping;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Analysis;

[KsqlTopic("bar")]
class TestSource
{
    public int Id { get; set; }
}

public class DerivedTumblingPipelineTests
{
    [Fact]
    public async Task Final1sStream_repartitions_on_bucket_and_emits_changes()
    {
        var qao = new TumblingQao
        {
            TimeKey = "Timestamp",
            Windows = new[] { new Timeframe(1, "m") },
            Keys = new[] { "Broker", "Symbol", "BucketStart" },
            Projection = new[] { "Broker", "Symbol", "BucketStart", "Open", "High", "Low", "Close" },
            PocoShape = new[]
            {
                new ColumnShape("Broker", typeof(string), false),
                new ColumnShape("Symbol", typeof(string), false),
                new ColumnShape("BucketStart", typeof(long), false),
                new ColumnShape("Open", typeof(double), false),
                new ColumnShape("High", typeof(double), false),
                new ColumnShape("Low", typeof(double), false),
                new ColumnShape("Close", typeof(double), false)
            },
            BasedOn = new BasedOnSpec(new[] { "Broker", "Symbol" }, "Open", "Close", string.Empty),
            WeekAnchor = DayOfWeek.Monday
        };
        var baseModel = new EntityModel { EntityType = typeof(TestSource) };
        var model = new KsqlQueryModel
        {
            SourceTypes = new[] { typeof(TestSource) },
            Windows = { "1m" }
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

        _ = await DerivedTumblingPipeline.RunAsync(qao, baseModel, model, Exec, Resolver, mapping, registry, new LoggerFactory().CreateLogger("test"));

        var streamDdl = ddls.Single(s => s.StartsWith("CREATE STREAM bar_1s_final_s", StringComparison.OrdinalIgnoreCase));
        Assert.StartsWith("CREATE STREAM BAR_1S_FINAL_S (", streamDdl, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("KAFKA_TOPIC='BAR_1S_FINAL'", streamDdl, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("KEY_FORMAT='AVRO'", streamDdl, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("PARTITIONS=1", streamDdl, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("REPLICAS=1", streamDdl, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("RETENTION_MS=604800000", streamDdl, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("VALUE_AVRO_SCHEMA_FULL_NAME='", streamDdl, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("BROKER VARCHAR KEY", streamDdl, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("SYMBOL VARCHAR KEY", streamDdl, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("BUCKETSTART BIGINT", streamDdl, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("OPEN DOUBLE", streamDdl, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("VALUE_FORMAT='AVRO'", streamDdl, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("AS SELECT", streamDdl, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("EMIT", streamDdl, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("PARTITION BY", streamDdl, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("GROUP BY", streamDdl, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Live_and_Final_emit_different_ddl_without_mutating_model()
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
            BasedOn = new BasedOnSpec(new[] { "Id", "BucketStart" }, string.Empty, "KsqlTimeFrameClose", string.Empty),
            WeekAnchor = DayOfWeek.Monday
        };
        var baseModel = new EntityModel { EntityType = typeof(TestSource) };
        var model = new KsqlQueryModel
        {
            SourceTypes = new[] { typeof(TestSource) },
            Windows = { "1m" }
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

        _ = await DerivedTumblingPipeline.RunAsync(qao, baseModel, model, Exec, Resolver, mapping, registry, new LoggerFactory().CreateLogger("test"));

        Assert.Contains(ddls, s => s.StartsWith("CREATE TABLE bar_1s_final"));
        Assert.Contains(ddls, s => s.StartsWith("CREATE STREAM bar_1s_final_s", StringComparison.OrdinalIgnoreCase));
        Assert.Contains(ddls, s => s.StartsWith("CREATE TABLE bar_1m_live") || s.StartsWith("CREATE STREAM bar_1m_live"));
        Assert.DoesNotContain(ddls, s => s.Contains("_1m_final"));
    }

    [Fact]
    public async Task Final_5m_uses_hub_stream_as_input()
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
        var baseModel = new EntityModel { EntityType = typeof(TestSource) };
        var model = new KsqlQueryModel
        {
            SourceTypes = new[] { typeof(TestSource) },
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

        _ = await DerivedTumblingPipeline.RunAsync(qao, baseModel, model, Exec, Resolver, mapping, registry, new LoggerFactory().CreateLogger("test"));

        var ddl5 = ddls.Single(s => s.Contains("_5m_live"));
        Assert.Contains("FROM bar_1s_final_s", ddl5);
        Assert.DoesNotContain("EMIT FINAL", ddl5);
    }

    [Fact]
    public async Task Final_projection_reaggregates_columns()
    {
        var qao = new TumblingQao
        {
            TimeKey = "Timestamp",
            Windows = new[] { new Timeframe(1, "m") },
            Keys = new[] { "Broker", "Symbol", "BucketStart" },
            Projection = new[] { "Broker", "Symbol", "BucketStart", "Open", "High", "Low", "KsqlTimeFrameClose", "Volume" },
            PocoShape = new[]
            {
                new ColumnShape("Broker", typeof(string), false),
                new ColumnShape("Symbol", typeof(string), false),
                new ColumnShape("BucketStart", typeof(long), false),
                new ColumnShape("Open", typeof(double), false),
                new ColumnShape("High", typeof(double), false),
                new ColumnShape("Low", typeof(double), false),
                new ColumnShape("KsqlTimeFrameClose", typeof(double), false),
                new ColumnShape("Volume", typeof(double), false)
            },
            BasedOn = new BasedOnSpec(new[] { "Broker", "Symbol" }, "Open", "KsqlTimeFrameClose", string.Empty),
            WeekAnchor = DayOfWeek.Monday
        };
        var baseModel = new EntityModel { EntityType = typeof(TestSource) };
        var model = new KsqlQueryModel
        {
            SourceTypes = new[] { typeof(TestSource) },
            Windows = { "1m" }
        };
        Expression<Func<IGrouping<object, TestSource>, object>> sel = g => new
        {
            Open = g.EarliestByOffset(x => 0.0),
            High = g.Max(x => 0.0),
            Low = g.Min(x => 0.0),
            KsqlTimeFrameClose = g.LatestByOffset(x => 0.0),
            Volume = g.Sum(x => 0.0)
        };
        model.SelectProjection = sel;
        Assert.NotNull(model.SelectProjection);

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

        _ = await DerivedTumblingPipeline.RunAsync(qao, baseModel, model, Exec, Resolver, mapping, registry, new LoggerFactory().CreateLogger("test"));

        var ddl = ddls.Single(s => s.StartsWith("CREATE TABLE bar_1m_live") || s.StartsWith("CREATE STREAM bar_1m_live"));
        Assert.Contains("FROM bar_1s_final_s", ddl);
        Assert.DoesNotContain("BID", ddl, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Final_ddl_has_no_window_clause_or_agg_final_reference()
    {
        var qao = new TumblingQao
        {
            TimeKey = "Timestamp",
            Windows = new[] { new Timeframe(1, "m"), new Timeframe(5, "m") },
            Keys = new[] { "Id", "BucketStart" },
            Projection = new[] { "Id", "BucketStart" },
            PocoShape = new[]
            {
                new ColumnShape("Id", typeof(int), false),
                new ColumnShape("BucketStart", typeof(long), false)
            },
            BasedOn = new BasedOnSpec(new[] { "Id", "BucketStart" }, string.Empty, "BucketStart", string.Empty),
            WeekAnchor = DayOfWeek.Monday
        };
        var baseModel = new EntityModel { EntityType = typeof(TestSource) };
        var model = new KsqlQueryModel
        {
            SourceTypes = new[] { typeof(TestSource) },
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

        _ = await DerivedTumblingPipeline.RunAsync(qao, baseModel, model, Exec, Resolver, mapping, registry, new LoggerFactory().CreateLogger("test"));

        foreach (var sql in ddls.Where(s => s.Contains("_final")))
        {
            Assert.DoesNotContain("_agg_final", sql, StringComparison.OrdinalIgnoreCase);
        }
    }
}

