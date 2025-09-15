using Kafka.Ksql.Linq.Query.Builders;
using System;
using System.Linq.Expressions;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Builders.Visitors;

public class AggregateDetectionVisitorTests
{
    private static class Agg
    {
        public static int Sum(int value) => value;
    }

    private class Sample
    {
        public int Amount { get; set; }
        public int Value { get; set; }
    }

    [Fact]
    public void Visit_WithAggregateMethod_DetectsAggregate()
    {
        Expression<Func<Sample, int>> expr = e => e.Amount + Agg.Sum(e.Value);
        var visitor = new AggregateDetectionVisitor();
        visitor.Visit(expr.Body);
        Assert.True(visitor.HasAggregates);
    }

    [Fact]
    public void Visit_WithoutAggregateMethod_NoDetection()
    {
        Expression<Func<Sample, int>> expr = e => e.Amount + e.Value;
        var visitor = new AggregateDetectionVisitor();
        visitor.Visit(expr.Body);
        Assert.False(visitor.HasAggregates);
    }
}
