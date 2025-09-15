using Kafka.Ksql.Linq.Query.Builders;
using System;
using System.Linq.Expressions;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Builders;

public class GroupByClauseBuilderTests
{
    [Fact]
    public void Build_SingleKey_ReturnsKeyName()
    {
        Expression<Func<TestEntity, object>> expr = e => e.Type;
        var builder = new GroupByClauseBuilder();
        var sql = builder.Build(expr.Body);
        Assert.Equal("Type", sql);
    }

    [Fact]
    public void Build_CompositeKey_ReturnsCommaSeparatedKeys()
    {
        Expression<Func<TestEntity, object>> expr = e => new { e.Id, e.Type };
        var builder = new GroupByClauseBuilder();
        var sql = builder.Build(expr.Body);
        Assert.Equal("test-topic.ID, Type", sql);
    }

    [Fact]
    public void Build_WithAlias_UsesAliasForKeyPrefix()
    {
        Expression<Func<TestEntity, object>> expr = e => e.Id;
        var map = new System.Collections.Generic.Dictionary<string, string> { ["e"] = "t" };
        var builder = new GroupByClauseBuilder(map);
        var sql = builder.Build(expr.Body);
        Assert.Equal("t.ID", sql);

    }
}
