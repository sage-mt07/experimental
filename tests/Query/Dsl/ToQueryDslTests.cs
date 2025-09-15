using Kafka.Ksql.Linq;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Core.Modeling;
using Kafka.Ksql.Linq.Query.Builders;
using Kafka.Ksql.Linq.Query.Dsl;
using System;
using System.Linq;
using System.Linq.Expressions;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Dsl;

public class ToQueryDslTests
{
    private class Order
    {
        [KsqlKey]
        public int Id { get; set; }
        public int CustomerId { get; set; }
        public double Amount { get; set; }
    }

    private class Customer
    {
        [KsqlKey]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public bool IsActive { get; set; }
    }

    private class OrderView
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private class OrderAmountString
    {
        [KsqlKey]
        public int Id { get; set; }
        public string Amount { get; set; } = string.Empty;
    }

    private class OrderDecimal
    {
        [KsqlKey]
        public int Id { get; set; }
        [KsqlDecimal(18, 2)]
        public decimal Amount { get; set; }
    }

    private class OrderDecimalScaled
    {
        [KsqlKey]
        public int Id { get; set; }
        [KsqlDecimal(18, 4)]
        public decimal Amount { get; set; }
    }

    [KsqlTable]
    private class OrderTable
    {
        [KsqlKey]
        public int Id { get; set; }
        public double Amount { get; set; }
    }

    [KsqlTable]
    private class QuoteTable
    {
        [KsqlKey]
        public string Broker { get; set; } = string.Empty;
        [KsqlKey]
        public string Symbol { get; set; } = string.Empty;
        public double Price { get; set; }
    }

    private class KeylessView
    {
        public string Name { get; set; } = string.Empty;
    }

    private class FakeQueryable : IKsqlQueryable
    {
        public KsqlQueryModel Build() => new KsqlQueryModel
        {
            SourceTypes = new[] { typeof(Order), typeof(Customer), typeof(OrderView) }
        };
    }

    [Fact]
    public void FromOnly_GeneratesSelectAll()
    {
        var model = new KsqlQueryRoot()
            .From<Order>()
            .Build();

        var sql = KsqlCreateStatementBuilder.Build("orders", model);
        Assert.Contains("FROM Order", sql);
        Assert.Contains("SELECT *", sql);
    }

    [Fact]
    public void FromSelect_GeneratesColumnList()
    {
        var model = new KsqlQueryRoot()
            .From<Order>()
            .Select(o => new { o.Id })
            .Build();

        var sql = KsqlCreateStatementBuilder.Build("orders", model);
        Assert.Contains("SELECT ID AS Id", sql);
    }

    [Fact]
    public void JoinSelect_GeneratesJoinClause()
    {
        var model = new KsqlQueryRoot()
            .From<Order>()
            .Join<Customer>((o, c) => o.CustomerId == c.Id)
            .Where((o, c) => c.IsActive)
            .Select((o, c) => new { o.Id, c.Name })
            .Build();

        var sql = KsqlCreateStatementBuilder.Build("view", model);
        Assert.Contains("JOIN Customer", sql);
        Assert.Contains("ON (o.CustomerId = i.Id)", sql);
    }

    [Fact]
    public void JoinWhereSelect_GeneratesWhere()
    {
        var model = new KsqlQueryRoot()
            .From<Order>()
            .Join<Customer>((o, c) => o.CustomerId == c.Id)
            .Where((o, c) => c.IsActive)
            .Select((o, c) => new { o.Id, c.Name })
            .Build();

        var sql = KsqlCreateStatementBuilder.Build("view", model);
        Assert.Contains("WHERE", sql);
        Assert.Contains("IsActive", sql);
    }

    [Fact]
    public void KeylessEntity_AllowsNoKeys()
    {
        var builder = new ModelBuilder();
        builder.Entity<Order>();
        var entityBuilder = (EntityModelBuilder<KeylessView>)builder.Entity<KeylessView>();

        entityBuilder.ToQuery(q => q.From<Order>()
            .Select(o => new KeylessView { Name = "x" }));

        var model = entityBuilder.GetModel();
        Assert.NotNull(model.QueryModel);
    }

    [Fact]
    public void KeyMismatch_Throws()
    {
        var builder = new ModelBuilder();
        builder.Entity<Order>();
        var entityBuilder = builder.Entity<OrderView>();

        Assert.Throws<InvalidOperationException>(() =>
            entityBuilder.ToQuery(q => q.From<Order>()
                .Select(o => new { o.CustomerId })));
    }

    [Fact]
    public void TypeMismatch_Throws()
    {
        var builder = new ModelBuilder();
        builder.Entity<Order>();
        var entityBuilder = builder.Entity<OrderAmountString>();

        Assert.Throws<InvalidOperationException>(() =>
            entityBuilder.ToQuery(q => q.From<Order>()
                .Select(o => new { o.Id, o.Amount })));
    }

    [Fact]
    public void DecimalPrecisionMismatch_Throws()
    {
        var builder = new ModelBuilder();
        builder.Entity<OrderDecimal>();
        var entityBuilder = builder.Entity<OrderDecimalScaled>();

        Assert.Throws<InvalidOperationException>(() =>
            entityBuilder.ToQuery(q => q.From<OrderDecimal>()
                .Select(o => new { o.Id, o.Amount })));
    }

    [Fact]
    public void SelectOrder_AffectsSqlColumnOrder()
    {
        var model = new KsqlQueryRoot()
            .From<Order>()
            .Select(o => new { Name = o.CustomerId.ToString(), Id = o.Id })
            .Build();

        var sql = KsqlCreateStatementBuilder.Build("orders", model);
        var selectLine = sql.Split('\n')[1];
        Assert.Contains("Name", selectLine);
        var nameIndex = selectLine.IndexOf("Name");
        var idIndex = selectLine.IndexOf("Id", nameIndex + 1);
        Assert.True(nameIndex < idIndex);
    }

    [Fact]
    public void ThreeTableJoin_Throws()
    {
        var builder = new ModelBuilder();
        Assert.Throws<NotSupportedException>(() =>
            builder.Entity<OrderView>().ToQuery(_ => new FakeQueryable()));
    }

    [Fact]
    public void WhereAfterSelect_Throws()
    {
        var query = new KsqlQueryRoot()
            .From<Order>()
            .Select(o => new { o.Id });

        Assert.Throws<InvalidOperationException>(() =>
            query.Where(o => o.Id > 0));
    }

    [Fact]
    public void Tumbling_NotSupported()
    {
        var query = new KsqlQueryRoot()
            .From<Order>();

        Assert.Throws<NotSupportedException>(() =>
            query.Tumbling(o => o.Id, TimeSpan.FromMinutes(1)));
    }

    [Fact]
    public void GroupBySelect_GeneratesGroupByClause()
    {
        var model = new KsqlQueryRoot()
            .From<Order>()
            .GroupBy(o => o.CustomerId)
            .Select(g => new { g.Key, Count = g.Count() })
            .Build();

        var sql = KsqlCreateStatementBuilder.Build("orders", model);
        Assert.Contains("GROUP BY CustomerId", sql);
        Assert.Contains("COUNT(", sql);
    }

    [Fact]
    public void GroupByKey_RendersKeyWithoutAlias()
    {
        var model = new KsqlQueryRoot()
            .From<Order>()
            .GroupBy(o => o.Id)
            .Select(g => new { g.Key })
            .Build();

        var sql = KsqlCreateStatementBuilder.Build("orders", model);
        Assert.Contains("GROUP BY ID", sql);
        Assert.Contains("SELECT ID AS ID", sql);
    }

    [Fact]
    public void KeyPathStyle_Arrow_RendersKeyArrowForTable()
    {
        var model = new KsqlQueryRoot()
            .From<Order>()
            .GroupBy(o => o.Id)
            .Select(g => new { g.Key })
            .Build();

        var sql = KsqlCreateStatementBuilder.Build("orders", model, options: new RenderOptions { KeyPathStyle = KeyPathStyle.Arrow });
        Assert.Contains("GROUP BY KEY->ID", sql);
        Assert.Contains("SELECT KEY->ID AS ID", sql);
    }

    [Fact]
    public void TableKey_RendersKeyArrowAutomatically()
    {
        var model = new KsqlQueryRoot()
            .From<OrderTable>()
            .GroupBy(o => o.Id)
            .Select(g => new { g.Key })
            .Build();

        var sql = KsqlCreateStatementBuilder.Build("orders", model);
        Assert.Contains("GROUP BY KEY->ID", sql);
        Assert.Contains("SELECT KEY->ID AS ID", sql);
    }

    [Fact]
    public void TableCompositeKey_RendersArrowForEachKey()
    {
        var model = new KsqlQueryRoot()
            .From<QuoteTable>()
            .GroupBy(q => new { q.Broker, q.Symbol })
            .Select(g => new { g.Key.Broker, g.Key.Symbol })
            .Build();

        var sql = KsqlCreateStatementBuilder.Build("quotes", model);
        Assert.Contains("GROUP BY KEY->BROKER, KEY->SYMBOL", sql);
        Assert.Contains("SELECT KEY->BROKER AS Broker, KEY->SYMBOL AS Symbol", sql);
    }

    [Fact]
    public void StreamTableJoin_AppliesArrowOnlyToTableSide()
    {
        var model = new KsqlQueryModel
        {
            SourceTypes = new[] { typeof(OrderTable), typeof(Customer) },
            JoinCondition = (Expression<Func<OrderTable, Customer, bool>>)((o, c) => o.Id == c.Id),
            WhereCondition = (Expression<Func<OrderTable, Customer, bool>>)((o, c) => o.Id > 0 && c.Id > 0),
            GroupByExpression = (Expression<Func<OrderTable, Customer, object>>)((o, c) => new { o.Id, c.Name }),
            SelectProjection = (Expression<Func<OrderTable, Customer, object>>)((o, c) => new { o.Id, c.Name }),
            WithinSeconds = 300
        };

        var sql = KsqlCreateStatementBuilder.Build("view", model);
        Assert.Contains("ON (KEY->ID = i.Id)", sql);
        Assert.Contains("SELECT KEY->ID AS Id", sql);
        Assert.Contains("GROUP BY KEY->ID, Name", sql);
        Assert.DoesNotContain("KEY->Name", sql);
        Assert.Contains("WHERE ((KEY->ID > 0) AND (ID > 0))", sql);
    }

    [Fact]
    public void WhereClause_KeyArrowApplied()
    {
        var model = new KsqlQueryRoot()
            .From<OrderTable>()
            .Where(o => o.Id > 0)
            .Select(o => new { o.Amount })
            .Build();

        var sql = KsqlCreateStatementBuilder.Build("orders", model);
        Assert.Contains("WHERE (KEY->ID > 0)", sql);
    }

    [Fact]
    public void HavingClause_KeyArrowApplied()
    {
        var model = new KsqlQueryRoot()
            .From<OrderTable>()
            .GroupBy(o => o.Id)
            .Having(g => g.Key > 1)
            .Select(g => new { g.Key })
            .Build();

        var sql = KsqlCreateStatementBuilder.Build("orders", model);
        Assert.Contains("HAVING (KEY->ID > 1)", sql);
    }

    [Fact]
    public void GroupByHaving_GeneratesHavingClause()
    {
        var model = new KsqlQueryRoot()
            .From<Order>()
            .GroupBy(o => o.CustomerId)
            .Having(g => g.Count() > 1)
            .Select(g => new { g.Key, Count = g.Count() })
            .Build();

        var sql = KsqlCreateStatementBuilder.Build("orders", model);
        Assert.Contains("HAVING", sql);
        Assert.Contains("COUNT(*) > 1", sql);
    }

    [Fact]
    public void GroupBySelectWithCase_GeneratesCaseExpression()
    {
        var model = new KsqlQueryRoot()
            .From<Order>()
            .GroupBy(o => o.CustomerId)
            .Select(g => new { g.Key, Status = g.Sum(x => x.Amount) > 100 ? "VIP" : "Regular" })
            .Build();

        var sql = KsqlCreateStatementBuilder.Build("orders", model);
        Assert.Contains("CASE WHEN", sql);
        Assert.Contains("SUM(", sql);
    }

    [Fact]
    public void HavingAfterSelect_Throws()
    {
        var query = new KsqlQueryRoot()
            .From<Order>()
            .GroupBy(o => o.CustomerId)
            .Select(g => new { g.Key });

        Assert.Throws<InvalidOperationException>(() =>
            query.Having(g => g.Count() > 1));
    }

    [Fact]
    public void SqlClauseOrder_IsCorrect()
    {
        var model = new KsqlQueryRoot()
            .From<Order>()
            .Where(o => o.Amount > 0)
            .GroupBy(o => o.CustomerId)
            .Having(g => g.Count() > 1)
            .Select(g => new { g.Key, Count = g.Count() })
            .Build();

        var sql = KsqlCreateStatementBuilder.Build("orders", model);
        var fromIdx = sql.IndexOf("FROM");
        var whereIdx = sql.IndexOf("WHERE");
        var groupIdx = sql.IndexOf("GROUP BY");
        var havingIdx = sql.IndexOf("HAVING");

        Assert.True(fromIdx < whereIdx);
        Assert.True(whereIdx < groupIdx);
        Assert.True(groupIdx < havingIdx);
    }

    [Fact]
    public void SourceNameResolver_Replaces_FromAndJoin_Names()
    {
        var model = new KsqlQueryRoot()
            .From<Order>()
            .Join<Customer>((o, c) => o.CustomerId == c.Id)
            .Select((o, c) => new { o.Id, c.Name })
            .Build();

        string Resolver(Type t) => t == typeof(Order) ? "ORDERS" : t == typeof(Customer) ? "CUSTOMERS" : t.Name;

        var sql = KsqlCreateStatementBuilder.Build("view", model, null, null, Resolver);
        Assert.Contains("FROM ORDERS", sql);
        Assert.Contains("JOIN CUSTOMERS", sql);
    }
}
