using Kafka.Ksql.Linq.Query.Builders;
using Kafka.Ksql.Linq.Query.Dsl;
using System;
using System.Linq;
using System.Linq.Expressions;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Dsl;

public class KsqlQueryable2Tests
{
    private class Order
    {
        public int Id { get; set; }
        public int Amount { get; set; }
    }

    private class Payment
    {
        public int OrderId { get; set; }
        public int Paid { get; set; }
    }

    [Fact]
    public void Select_WithAggregate_SetsAggregateFlag()
    {
        Expression<Func<Order, Payment, object>> projection = (o, p) => new { Total = o.Amount + p.Paid, Sum = Enumerable.Sum(new[] { o.Amount, p.Paid }) };
        var queryable = new KsqlQueryable2<Order, Payment>().Select(projection);
        var model = queryable.Build();
        Assert.True(model.IsAggregateQuery());
    }

    [Fact(Skip = "Requires join condition setup")]
    public void BuildCreateStatement_UsesCreateTableForAggregates()
    {
        Expression<Func<Order, Payment, object>> projection = (o, p) => new { Count = new int[] { o.Amount }.Count() };
        var queryable = new KsqlQueryable2<Order, Payment>().Select(projection);
        var model = queryable.Build();
        var sql = KsqlCreateStatementBuilder.Build("Summary", model);
        Assert.Contains("CREATE TABLE Summary", sql);
    }

    [Fact]
    public void Join_WithWithin_AddsClause()
    {
        var queryable = new KsqlQueryable<Order>()
            .Join<Payment>((o, p) => o.Id == p.OrderId)
            .Within(TimeSpan.FromSeconds(300))
            .Select((o, p) => new { o.Id, p.Paid });
        var model = queryable.Build();
        var sql = KsqlCreateStatementBuilder.Build("JoinTest", model);
        Assert.Contains("WITHIN 300 SECONDS", sql);
        Assert.Contains("SELECT o.Id AS Id, i.Paid AS Paid", sql);
        Assert.Contains("ON (o.Id = i.OrderId)", sql);
    }

    [Fact(Skip = "Within check not enforced")]
    public void Join_WithoutWithin_Throws()
    {
        var queryable = new KsqlQueryable<Order>()
            .Join<Payment>((o, p) => o.Id == p.OrderId)
            .Select((o, p) => new { o.Id, p.Paid });
        var model = queryable.Build();
        var ex = Assert.Throws<InvalidOperationException>(() => KsqlCreateStatementBuilder.Build("JoinTest", model));
        Assert.Contains("Within(60)", ex.Message);
    }

    [Fact]
    public void Select_WithUnqualifiedColumn_Throws()
    {
        var outside = new Order();
        var queryable = new KsqlQueryable<Order>()
            .Join<Payment>((o, p) => o.Id == p.OrderId)
            .Within(TimeSpan.FromSeconds(5))
            .Select((o, p) => new { outside.Id, p.Paid });
        var model = queryable.Build();
        Assert.Throws<InvalidOperationException>(() => KsqlCreateStatementBuilder.Build("JoinTest", model));
    }

    [Fact]
    public void Join_WithUnqualifiedColumnInCondition_Throws()
    {
        var outside = new Order();
        var queryable = new KsqlQueryable<Order>()
            .Join<Payment>((o, p) => outside.Id == p.OrderId)
            .Within(TimeSpan.FromSeconds(5))
            .Select((o, p) => new { o.Id, p.Paid });
        var model = queryable.Build();
        Assert.Throws<InvalidOperationException>(() => KsqlCreateStatementBuilder.Build("JoinTest", model));
    }
}
