using Kafka.Ksql.Linq.Query.Builders;
using Kafka.Ksql.Linq.Query.Dsl;
using System;
using System.Linq;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Dsl;

public class KsqlCreateStatementBuilderDslTests
{
    private class Order { public int Id { get; set; } public int CustomerId { get; set; } }
    private class Customer { public int Id { get; set; } public bool IsActive { get; set; } public string Name { get; set; } = string.Empty; }

    [Fact]
    public void Build_WithJoinWhereSelect_GeneratesKsql()
    {
        var model = new KsqlQueryRoot()
            .From<Order>()
            .Join<Customer>((o, c) => o.CustomerId == c.Id)
            .Within(TimeSpan.FromSeconds(5))
            .Where((o, c) => c.IsActive)
            .Select((o, c) => new { o.Id, c.Name })
            .Build();

        var sql = KsqlCreateStatementBuilder.Build("JoinView", model, "com.acme.Key", "com.acme.Value");
        Assert.Contains("JOIN Customer", sql);
        Assert.Contains("WHERE", sql);
        Assert.Contains("SELECT", sql);
        Assert.Contains("KEY_FORMAT='AVRO'", sql);
        Assert.Contains("VALUE_AVRO_SCHEMA_FULL_NAME='com.acme.Value'", sql);
    }

    [Fact]
    public void Build_JoinWithoutWhere_GeneratesSql()
    {
        var model = new KsqlQueryRoot()
            .From<Order>()
            .Join<Customer>((o, c) => o.CustomerId == c.Id)
            .Within(TimeSpan.FromSeconds(5))
            .Select((o, c) => new { o.Id, c.Name })
            .Build();

        var sql = KsqlCreateStatementBuilder.Build("JoinView", model);
        Assert.Contains("JOIN Customer", sql);
        Assert.DoesNotContain("WHERE", sql);
    }

    [Fact]
    public void Build_Internal_OmitsKeySerDe()
    {
        var model = new KsqlQueryRoot()
            .From<Order>()
            .Select(o => new { o.Id })
            .Build();

        var sql = KsqlCreateStatementBuilder.Build("orders", model, null, "com.acme.Value");
        Assert.Contains("KEY_FORMAT='AVRO'", sql);
        Assert.DoesNotContain("KEY_AVRO_SCHEMA_FULL_NAME", sql);
        Assert.Contains("VALUE_AVRO_SCHEMA_FULL_NAME='com.acme.Value'", sql);
        Assert.DoesNotContain("PARTITION BY", sql);
    }

    [Fact]
    public void Build_AggregateQuery_UsesCreateTable()
    {
        var model = new KsqlQueryRoot()
            .From<Order>()
            .GroupBy(o => o.CustomerId)
            .Select(g => new { g.Key, Count = g.Count() })
            .Build();

        var sql = KsqlCreateStatementBuilder.Build("agg_view", model);
        Assert.StartsWith("CREATE TABLE", sql);
        Assert.Contains("GROUP BY CustomerId", sql);
    }

    [Fact]
    public void Build_Uses_DerivedEntityIds()
    {
        var model = new KsqlQueryRoot()
            .From<Order>()
            .Select(o => new { o.Id })
            .Build();

        var sql = KsqlCreateStatementBuilder.Build(
            "bar_1m_live",
            model,
            null,
            null,
            _ => "bar_1m_final");

        Assert.Contains("CREATE STREAM bar_1m_live", sql);
        Assert.Contains("FROM bar_1m_final", sql);
        Assert.DoesNotContain("Order", sql);
    }

    private static KsqlQueryModel BuildAggregateModel()
    {
        return new KsqlQueryRoot()
            .From<Order>()
            .Select(o => new { Count = new int[] { o.Id }.Count() })
            .Build();
    }
}
