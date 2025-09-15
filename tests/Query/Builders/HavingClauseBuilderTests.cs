using Kafka.Ksql.Linq.Query.Builders;
using System;
using System.Linq;
using System.Linq.Expressions;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Builders;

public class HavingClauseBuilderTests
{
    [Fact]
    public void Build_CountCondition_ReturnsCountExpression()
    {
        Expression<Func<IGrouping<int, TestEntity>, bool>> expr = g => g.Count() > 1;
        var builder = new HavingClauseBuilder();
        var sql = builder.Build(expr.Body);
        Assert.Equal("(COUNT(*) > 1)", sql);
    }

    [Fact]
    public void Build_SumCondition_ReturnsSumExpression()
    {
        Expression<Func<IGrouping<int, TestEntity>, bool>> expr = g => g.Sum(x => x.Id) > 5;
        var builder = new HavingClauseBuilder();
        var sql = builder.Build(expr.Body);
        Assert.Equal("(SUM(Id) > 5)", sql);
    }

    private class Order
    {
        public double Amount { get; set; }
        public double Price { get; set; }
        public int Quantity { get; set; }
    }

    [Fact]
    public void Build_AndCondition_ReturnsCombinedExpression()
    {
        Expression<Func<IGrouping<int, Order>, bool>> expr =
            g => g.Count() > 10 && g.Sum(x => x.Amount) < 5000;

        var builder = new HavingClauseBuilder();
        var sql = builder.Build(expr.Body);

        Assert.Equal("((COUNT(*) > 10) AND (SUM(Amount) < 5000))", sql);
    }

    [Fact]
    public void Build_OrCondition_ReturnsCombinedExpression()
    {
        Expression<Func<IGrouping<int, Order>, bool>> expr =
            g => g.Average(x => x.Price) > 100 || g.Sum(x => x.Quantity) < 10;

        var builder = new HavingClauseBuilder();
        var sql = builder.Build(expr.Body);

        Assert.Equal("((AVG(Price) > 100) OR (SUM(Quantity) < 10))", sql);
    }

    [Fact]
    public void Build_NestedCondition_MaintainsParentheses()
    {
        Expression<Func<IGrouping<int, Order>, bool>> expr =
            g => (g.Count() > 5 && g.Sum(x => x.Amount) < 1000) || g.Sum(x => x.Amount) > 3000;

        var builder = new HavingClauseBuilder();
        var sql = builder.Build(expr.Body);

        Assert.Equal("(((COUNT(*) > 5) AND (SUM(Amount) < 1000)) OR (SUM(Amount) > 3000))", sql);
    }

    [Fact]
    public void Build_NotCondition_ReturnsNegatedExpression()
    {
        Expression<Func<IGrouping<int, Order>, bool>> expr =
            g => !(g.Count() < 3 || g.Sum(x => x.Amount) > 10000);

        var builder = new HavingClauseBuilder();
        var sql = builder.Build(expr.Body);

        Assert.Equal("NOT (((COUNT(*) < 3) OR (SUM(Amount) > 10000)))", sql);
    }
}
