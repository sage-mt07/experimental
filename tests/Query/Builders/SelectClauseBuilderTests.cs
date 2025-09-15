using Kafka.Ksql.Linq.Query.Builders;
using System;
using System.Linq;
using System.Linq.Expressions;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Builders;

public class SelectClauseBuilderTests
{
    [Fact]
    public void Build_MultipleColumns_ReturnsCommaSeparated()
    {
        Expression<Func<TestEntity, object>> expr = e => new { e.Id, e.Name };
        var builder = new SelectClauseBuilder();
        var sql = builder.Build(expr.Body);
        Assert.Equal("test-topic.ID AS Id, Name", sql);
    }

    [Fact]
    public void Build_WithAlias_ReturnsAliasedColumn()
    {
        Expression<Func<TestEntity, object>> expr = e => new { Alias = e.Name };
        var builder = new SelectClauseBuilder();
        var sql = builder.Build(expr.Body);
        Assert.Equal("Name AS Alias", sql);
    }

    [Fact]
    public void Build_WithFunction_ReturnsKsqlFunction()
    {
        Expression<Func<TestEntity, object>> expr = e => new { NameUpper = e.Name.ToUpper() };
        var builder = new SelectClauseBuilder();
        var sql = builder.Build(expr.Body);
        Assert.Equal("UPPER(Name) AS NameUpper", sql);
    }

    private class Order
    {
        public double Amount { get; set; }
    }

    [Fact]
    public void Build_AggregateFunctions_ReturnsAggregateExpressions()
    {
        Expression<Func<IGrouping<int, Order>, object>> expr =
            g => new { OrderCount = g.Count(), TotalAmount = g.Sum(x => x.Amount) };

        var builder = new SelectClauseBuilder();
        var sql = builder.Build(expr.Body);

        Assert.Equal("COUNT(*) AS OrderCount, SUM(Amount) AS TotalAmount", sql);
    }

    private class Bar
    {
        public DateTime BucketStart { get; set; }
    }

    [Fact]
    public void Build_MemberInit_WindowStart_AliasesProperty()
    {
        Expression<Func<IGrouping<int, TestEntity>, Bar>> expr = g => new Bar { BucketStart = g.WindowStart() };
        var builder = new SelectClauseBuilder();
        var sql = builder.Build(expr.Body);
        Assert.Equal("WINDOWSTART AS BucketStart", sql);
    }
}
