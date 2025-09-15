using System;
using System.Linq;
using System.Linq.Expressions;
using Kafka.Ksql.Linq.Query.Builders;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Builders;

public class ValidationErrorTests
{
    private class Order
    {
        public double Amount { get; set; }
    }

    [Fact]
    public void SelectClauseBuilder_NestedAggregates_Throws()
    {
        // g.Sum(x => g.Sum(y => y.Amount)) -> nested aggregate should be rejected
        Expression<Func<IGrouping<int, Order>, object>> expr =
            g => g.Sum(x => g.Sum(y => y.Amount));

        var builder = new SelectClauseBuilder();

        var ex = Assert.Throws<NotSupportedException>(() => builder.Build(expr.Body));
        Assert.Equal("Nested aggregate functions are not supported", ex.Message);
    }

    private class Flag
    {
        public bool IsActive { get; set; }
    }

    [Fact]
    public void WhereClauseBuilder_TooDeepExpression_Throws()
    {
        // Build a deeply nested NOT(NOT(...)) expression to exceed the depth limit (50)
        var p = Expression.Parameter(typeof(Flag), "f");
        Expression body = Expression.Property(p, nameof(Flag.IsActive));
        for (int i = 0; i < 55; i++)
        {
            body = Expression.Not(body);
        }

        var builder = new WhereClauseBuilder();
        var ex = Assert.Throws<InvalidOperationException>(() => builder.Build(body));
        Assert.StartsWith("Expression depth exceeds maximum allowed depth of 50.", ex.Message);
    }
}

public class MoreValidationErrorTests
{
    private class E
    {
        public int A { get; set; }
        public int B { get; set; }
    }

    [Fact]
    public void SelectClauseBuilder_MixAggregateAndNonAggregate_Throws()
    {
        // new { A = e.A, S = new[] { e.B }.Sum() } without GROUP BY -> invalid
        Expression<Func<E, object>> expr = e => new { A = e.A, S = new[] { e.B }.Sum() };
        var builder = new SelectClauseBuilder();
        var ex = Assert.Throws<InvalidOperationException>(() => builder.Build(expr.Body));
        Assert.Equal("SELECT clause cannot mix aggregate functions with non-aggregate columns without GROUP BY", ex.Message);
    }

    [Fact]
    public void WhereClauseBuilder_AggregateInWhere_Throws()
    {
        // e => Sum(...) > 0 is not allowed in WHERE
        Expression<Func<E, bool>> expr = e => new[] { e.A, e.B }.Sum() > 0;
        var builder = new WhereClauseBuilder();
        var ex = Assert.Throws<InvalidOperationException>(() => builder.Build(expr.Body));
        Assert.Equal("Aggregate functions are not allowed in WHERE clause. Use HAVING clause instead.", ex.Message);
    }
}
