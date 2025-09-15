using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Query.Dsl;
using Kafka.Ksql.Linq.Query.Abstractions;
using System;
using System.Linq.Expressions;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Builders;

public class KsqlCreateWindowedStatementBuilderTests
{
    private class Rate
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime Timestamp { get; set; }
        public double Bid { get; set; }
    }

    [KsqlTable]
    private class RateTable
    {
        [KsqlKey(0)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(1)] public string Symbol { get; set; } = string.Empty;
        public DateTime Timestamp { get; set; }
        public double Bid { get; set; }
    }

    [KsqlTopic("deduprates")]
    private class DedupRate
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime Ts { get; set; }
        public double Bid { get; set; }
    }

    [Fact]
    public void Build_Includes_Window_Tumbling_1m()
    {
        var model = new KsqlQueryRoot()
            .From<Rate>()
            .Tumbling(r => r.Timestamp, new Windows { Minutes = new[] { 1 } })
            .GroupBy(r => new { r.Broker, r.Symbol })
            .Select(g => new { g.Key.Broker, g.Key.Symbol, BucketStart = g.WindowStart(), Open = g.EarliestByOffset(x => x.Bid) })
            .Build();

        var sql = Kafka.Ksql.Linq.Query.Builders.KsqlCreateWindowedStatementBuilder.Build("bar_1m_live", model, "1m");
        Kafka.Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(sql, "WINDOW TUMBLING (SIZE 1 MINUTES)");
        Kafka.Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(sql, "CREATE TABLE bar_1m_live");
    }

    [Fact]
    public void Build_From_With_Alias_Inserts_Window_After_Alias()
    {
        var model = new KsqlQueryRoot()
            .From<DedupRate>()
            .Tumbling(r => r.Ts, new Windows { Minutes = new[] { 1 } })
            .GroupBy(r => new { r.Broker, r.Symbol })
            .Select(g => new { g.Key.Broker, g.Key.Symbol, BucketStart = g.WindowStart(), Open = g.EarliestByOffset(x => x.Bid) })
            .Build();

        var sql = Kafka.Ksql.Linq.Query.Builders.KsqlCreateWindowedStatementBuilder.Build(
            "bar_1m_live",
            model,
            "1m",
            null,
            "deduprates_1s_final_s");
        Kafka.Ksql.Linq.Tests.Utils.SqlAssert.AssertOrderNormalized(
            sql,
            "FROM deduprates_1s_final_s",
            "window tumbling"
        );
    }

    [Fact]
    public void BuildAll_Generates_Per_Window()
    {
        var model = new KsqlQueryRoot()
            .From<Rate>()
            .Tumbling(r => r.Timestamp, new Windows { Minutes = new[] { 1, 5 } })
            .GroupBy(r => new { r.Broker, r.Symbol })
            .Select(g => new { g.Key.Broker, g.Key.Symbol, BucketStart = g.WindowStart(), Open = g.EarliestByOffset(x => x.Bid) })
            .Build();

        var map = Kafka.Ksql.Linq.Query.Builders.KsqlCreateWindowedStatementBuilder.BuildAll(
            "bar",
            model,
            tf => $"bar_{tf}_live");

        Assert.True(map.ContainsKey("1m"));
        Assert.True(map.ContainsKey("5m"));
        Kafka.Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(map["1m"], "bar_1m_live");
        Kafka.Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(map["5m"], "bar_5m_live");
    }

    [Fact]
    public void Build_NoWindow_Creates_Stream()
    {
        var model = new KsqlQueryRoot()
            .From<Rate>()
            .Select(r => r)
            .Build();

        var sql = Kafka.Ksql.Linq.Query.Builders.KsqlCreateStatementBuilder.Build("rates", model);
        Kafka.Ksql.Linq.Tests.Utils.SqlAssert.StartsWithNormalized(sql, "CREATE STREAM rates");
    }

    [Fact]
    public void Build_WithWindow_Creates_Table()
    {
        var model = new KsqlQueryRoot()
            .From<Rate>()
            .Tumbling(r => r.Timestamp, new Windows { Minutes = new[] { 1 } })
            .GroupBy(r => new { r.Broker, r.Symbol })
            .Select(g => new { g.Key.Broker, g.Key.Symbol, BucketStart = g.WindowStart(), Open = g.EarliestByOffset(x => x.Bid) })
            .Build();

        var sql = Kafka.Ksql.Linq.Query.Builders.KsqlCreateWindowedStatementBuilder.Build("bar_1m", model, "1m");
        Kafka.Ksql.Linq.Tests.Utils.SqlAssert.StartsWithNormalized(sql, "CREATE TABLE bar_1m");
        Kafka.Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(sql, "WINDOW TUMBLING");
    }

    [Fact]
    public void DetermineType_Tumbling_Returns_Table()
    {
        var model = new KsqlQueryRoot()
            .From<Rate>()
            .Tumbling(r => r.Timestamp, new Windows { Minutes = new[] { 1 } })
            .GroupBy(r => new { r.Broker, r.Symbol })
            .Select(g => new { g.Key.Broker, g.Key.Symbol, BucketStart = g.WindowStart() })
            .Build();
        Assert.Equal(StreamTableType.Table, model.DetermineType());
    }

    [Fact]
    public void DetermineType_NoAggregation_Returns_Stream()
    {
        var model = new KsqlQueryRoot()
            .From<Rate>()
            .Select(r => r)
            .Build();
        Assert.Equal(StreamTableType.Stream, model.DetermineType());
    }
}

