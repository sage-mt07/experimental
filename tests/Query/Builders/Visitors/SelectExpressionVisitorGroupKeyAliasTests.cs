using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Query.Builders;
using System;
using System.Linq;
using System.Linq.Expressions;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Builders.Visitors;

public class SelectExpressionVisitorGroupKeyAliasTests
{
    private class Quote
    {
        [KsqlKey(order: 0)]
        public string Broker { get; set; } = string.Empty;
    }

    private class QuoteKey
    {
        public string Broker { get; set; } = string.Empty;
    }

    [Fact]
    public void VisitNew_GroupKeyProperty_NoDuplicateAlias()
    {
        Expression<Func<Quote, QuoteKey>> groupExpr = e => new QuoteKey { Broker = e.Broker };
        var groupBuilder = new GroupByClauseBuilder();
        groupBuilder.Build(groupExpr.Body);

        Expression<Func<IGrouping<QuoteKey, Quote>, object>> select = g => new { g.Key.Broker };
        var visitor = new SelectExpressionVisitor();
        visitor.Visit(select.Body);
        var result = visitor.GetResult();
        Assert.Equal("quote.BROKER AS Broker", result);
    }

    [Fact]
    public void VisitNew_GroupKeyProperty_WithAlias_UsesAsKeyword()
    {
        Expression<Func<Quote, QuoteKey>> groupExpr = e => new QuoteKey { Broker = e.Broker };
        var groupBuilder = new GroupByClauseBuilder();
        groupBuilder.Build(groupExpr.Body);

        Expression<Func<IGrouping<QuoteKey, Quote>, object>> select = g => new { b = g.Key.Broker };
        var visitor = new SelectExpressionVisitor();
        visitor.Visit(select.Body);
        var result = visitor.GetResult();
        Assert.Equal("quote.BROKER AS b", result);
    }

    private class QuoteFull
    {
        [KsqlKey(order: 0)]
        public string Broker { get; set; } = string.Empty;
        [KsqlKey(order: 1)]
        public string Symbol { get; set; } = string.Empty;
        public decimal Bid { get; set; }
    }

    private class QuoteKeyFull
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
    }

    private class Ohlc
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime BucketStart { get; set; }
        public decimal Open { get; set; }
        public decimal High { get; set; }
        public decimal Low { get; set; }
        public decimal Close { get; set; }
    }

    [Fact]
    public void VisitMemberInit_GroupKeysAndAggregates_EmitAsAliases()
    {
        Expression<Func<QuoteFull, QuoteKeyFull>> groupExpr = e => new QuoteKeyFull { Broker = e.Broker, Symbol = e.Symbol };
        var groupBuilder = new GroupByClauseBuilder();
        var groupClause = groupBuilder.Build(groupExpr.Body);
        Assert.Equal("quotefull.BROKER, quotefull.SYMBOL", groupClause);

        Expression<Func<IGrouping<QuoteKeyFull, QuoteFull>, Ohlc>> select = g => new Ohlc
        {
            Broker = g.Key.Broker,
            Symbol = g.Key.Symbol,
            BucketStart = g.WindowStart(),
            Open = g.EarliestByOffset(x => x.Bid),
            High = g.Max(x => x.Bid),
            Low = g.Min(x => x.Bid),
            Close = g.LatestByOffset(x => x.Bid)
        };

        var visitor = new SelectExpressionVisitor();
        visitor.Visit(select.Body);
        var result = visitor.GetResult();
        Assert.Equal("quotefull.BROKER AS Broker, quotefull.SYMBOL AS Symbol, WINDOWSTART AS BucketStart, EARLIEST_BY_OFFSET(Bid) AS Open, MAX(Bid) AS High, MIN(Bid) AS Low, LATEST_BY_OFFSET(Bid) AS Close", result);
    }

    [Fact]
    public void VisitMemberInit_GroupKeysAndAggregates_WithSourceAlias()
    {
        Expression<Func<QuoteFull, QuoteKeyFull>> groupExpr = q => new QuoteKeyFull { Broker = q.Broker, Symbol = q.Symbol };
        var map = new System.Collections.Generic.Dictionary<string, string> { ["q"] = "dedup" };
        var groupBuilder = new GroupByClauseBuilder(map);
        var groupClause = groupBuilder.Build(groupExpr.Body);
        Assert.Equal("dedup.BROKER, dedup.SYMBOL", groupClause);

        Expression<Func<IGrouping<QuoteKeyFull, QuoteFull>, Ohlc>> select = g => new Ohlc
        {
            Broker = g.Key.Broker,
            Symbol = g.Key.Symbol,
            BucketStart = g.WindowStart(),
            Open = g.EarliestByOffset(x => x.Bid),
            High = g.Max(x => x.Bid),
            Low = g.Min(x => x.Bid),
            Close = g.LatestByOffset(x => x.Bid)
        };

        var visitor = new SelectExpressionVisitor(new System.Collections.Generic.Dictionary<string, string> { ["g"] = "dedup" });
        visitor.Visit(select.Body);
        var result = visitor.GetResult();
        Assert.Equal("dedup.BROKER AS Broker, dedup.SYMBOL AS Symbol, WINDOWSTART AS BucketStart, EARLIEST_BY_OFFSET(Bid) AS Open, MAX(Bid) AS High, MIN(Bid) AS Low, LATEST_BY_OFFSET(Bid) AS Close", result);
    }

    [Fact]
    public void VisitMember_GroupKeyObject_WithAlias_UsesAliasPrefix()
    {
        Expression<Func<QuoteFull, QuoteKeyFull>> groupExpr = q => new QuoteKeyFull { Broker = q.Broker, Symbol = q.Symbol };
        var map = new System.Collections.Generic.Dictionary<string, string> { ["q"] = "dedup" };
        var groupBuilder = new GroupByClauseBuilder(map);
        groupBuilder.Build(groupExpr.Body);

        Expression<Func<IGrouping<QuoteKeyFull, QuoteFull>, object>> select = g => g.Key;
        var visitor = new SelectExpressionVisitor(new System.Collections.Generic.Dictionary<string, string> { ["g"] = "dedup" });
        visitor.Visit(select.Body);
        var result = visitor.GetResult();
        Assert.Equal("dedup.BROKER AS BROKER, dedup.SYMBOL AS SYMBOL", result);
    }
}
