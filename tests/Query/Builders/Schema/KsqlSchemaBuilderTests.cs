using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Query.Builders.Schema;
using System;
using System.Linq.Expressions;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Builders.Schema;

public class KsqlSchemaBuilderTests
{
    private class KsqlIgnoreAttribute : Attribute { }

    private class Order
    {
        public int Id { get; set; }
    }

    private class Product
    {
        public string Name { get; set; } = string.Empty;
    }

    private class OrderSummary
    {
        [KsqlKey]
        public int OrderId { get; set; }
        public string ProductName { get; set; } = string.Empty;
        [KsqlIgnore]
        public decimal Cost { get; set; }

        public OrderSummary(int orderId, string productName)
        {
            OrderId = orderId;
            ProductName = productName;
        }
    }

    [Fact]
    public void BuildSchema_ReturnsColumns()
    {
        Expression<Func<Order, Product, OrderSummary>> expr = (o, p) => new OrderSummary(o.Id, p.Name);

        var result = KsqlSchemaBuilder.BuildSchema(expr);
        Assert.Collection(result,
            c =>
            {
                Assert.Equal("OrderId", c.Name);
                Assert.Equal("INT", c.KsqlType);
                Assert.True(c.IsKey);
            },
            c =>
            {
                Assert.Equal("ProductName", c.Name);
                Assert.Equal("VARCHAR", c.KsqlType);
                Assert.False(c.IsKey);
            });
    }
}
