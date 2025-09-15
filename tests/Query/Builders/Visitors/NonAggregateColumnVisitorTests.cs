using Kafka.Ksql.Linq.Query.Builders;
using System;
using System.Linq;
using System.Linq.Expressions;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Builders.Visitors;

public class NonAggregateColumnVisitorTests
{
    [Fact]
    public void Visit_MemberOutsideAggregate_SetsFlag()
    {
        Expression<Func<TestEntity, int>> expr = e => e.Id;
        var visitor = new NonAggregateColumnVisitor();
        visitor.Visit(expr.Body);
        Assert.True(visitor.HasNonAggregateColumns);
    }

    [Fact]
    public void Visit_MemberInsideAggregate_DoesNotSetFlag()
    {
        Expression<Func<int>> expr = () => new[] { 1 }.Sum();
        var visitor = new NonAggregateColumnVisitor();
        visitor.Visit(expr.Body);
        Assert.False(visitor.HasNonAggregateColumns);
    }

    [Fact]
    public void Visit_MixedExpression_OnlyCountsNonAggregated()
    {
        Expression<Func<TestEntity, int>> expr = e => e.Id + new[] { e.Id }.Sum();
        var visitor = new NonAggregateColumnVisitor();
        visitor.Visit(expr.Body);
        Assert.True(visitor.HasNonAggregateColumns);
    }

    private class TestEntity
    {
        public int Id { get; set; }
    }
}
