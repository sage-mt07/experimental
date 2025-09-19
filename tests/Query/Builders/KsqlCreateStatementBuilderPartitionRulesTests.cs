using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Query.Builders;
using Kafka.Ksql.Linq.Query.Dsl;
using Kafka.Ksql.Linq.Tests.Utils;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Builders;

public class KsqlCreateStatementBuilderPartitionRulesTests
{
    private const string EmitFinalFlag = "EMIT FINAL";

    private class Quote
    {
        [KsqlKey]
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public decimal Price { get; set; }
        public DateTime Timestamp { get; set; }
    }

    private class QuoteWithoutKey
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public decimal Price { get; set; }
    }

    [KsqlTable]
    private class QuoteTable : Quote { }

    [Fact]
    public void DoesNotInjectPartition_WhenGroupBy()
    {
        var model = new KsqlQueryRoot()
            .From<Quote>()
            .GroupBy(q => q.Broker)
            .Select(g => new { g.Key, Count = g.Count() })
            .Build();

        var sql = KsqlCreateStatementBuilder.Build("BROKER_COUNTS", model, partitionBy: "o.Symbol");

        SqlAssert.ContainsNormalized(sql, "GROUP BY Broker");
        Assert.DoesNotContain("PARTITION BY", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("CREATE TABLE", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void DoesNotInjectPartition_WhenWindowOrEmitFinal()
    {
        var windowed = new KsqlQueryRoot()
            .From<Quote>()
            .Tumbling(q => q.Timestamp, new Windows { Minutes = new[] { 1 } })
            .Select(q => new { q.Broker, q.Symbol, q.Price })
            .Build();

        var windowSql = KsqlCreateStatementBuilder.Build("QUOTES_1M", windowed, partitionBy: "o.Symbol");
        Assert.DoesNotContain("PARTITION BY", windowSql, StringComparison.OrdinalIgnoreCase);

        var emitFinal = new KsqlQueryRoot()
            .From<Quote>()
            .Select(q => new { q.Broker, q.Symbol })
            .Build();
        emitFinal.Extras["emit"] = EmitFinalFlag;

        var finalSql = KsqlCreateStatementBuilder.Build("QUOTES_FINAL", emitFinal, partitionBy: "o.Symbol");
        Assert.DoesNotContain("PARTITION BY", finalSql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void InjectsPartition_WhenSingleStreamRekeys()
    {
        var rekeyed = new KsqlQueryRoot()
            .From<Quote>()
            .Select(q => new { q.Symbol, q.Broker })
            .Build();

        var sql = KsqlCreateStatementBuilder.Build("QUOTES_REKEYED", rekeyed, partitionBy: "o.Symbol, o.Broker, o.Symbol");

        Assert.Contains("CREATE TABLE", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("PARTITION BY", sql, StringComparison.OrdinalIgnoreCase);
        SqlAssert.ContainsNormalized(sql, "GROUP BY Broker, Symbol");

        var unknownKey = new KsqlQueryRoot()
            .From<QuoteWithoutKey>()
            .Select(q => new { q.Broker, q.Symbol })
            .Build();

        var unknownSql = KsqlCreateStatementBuilder.Build("QUOTES_REKEYED_UNKNOWN", unknownKey, partitionBy: "o.Broker");
        Assert.Contains("CREATE TABLE", unknownSql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("PARTITION BY", unknownSql, StringComparison.OrdinalIgnoreCase);
        SqlAssert.ContainsNormalized(unknownSql, "GROUP BY Broker");
    }

    [Fact]
    public void PartitionBy_Unqualified_Deduped_StableOrder()
    {
        var model = new KsqlQueryRoot()
            .From<Quote>()
            .Select(q => new { q.Broker, q.Symbol })
            .Build();

        var sql = KsqlCreateStatementBuilder.Build("QUOTES_DEDUP", model, partitionBy: "o.Symbol, o.Broker, o.Symbol, o.Broker");

        Assert.DoesNotContain("PARTITION BY", sql, StringComparison.OrdinalIgnoreCase);
        SqlAssert.ContainsNormalized(sql, "GROUP BY Broker, Symbol");
        Assert.DoesNotContain("o.", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Normalizes_Aliases_In_AllClauses()
    {
        var model = new KsqlQueryRoot()
            .From<Quote>()
            .Where(q => q.Price > 0 && q.Broker != string.Empty)
            .GroupBy(q => new { q.Broker, q.Symbol })
            .Having(g => g.Count() > 0)
            .Select(g => new
            {
                g.Key.Broker,
                g.Key.Symbol,
                Count = g.Count()
            })
            .Build();

        var sql = KsqlCreateStatementBuilder.Build("QUOTE_SUMMARY", model, partitionBy: "o.Symbol, o.Broker");

        SqlAssert.ContainsNormalized(sql, "SELECT Broker AS Broker, Symbol AS Symbol");
        SqlAssert.ContainsNormalized(sql, "COUNT(*) AS Count");
        SqlAssert.ContainsNormalized(sql, "GROUP BY Broker, Symbol");
        Assert.Contains("HAVING", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("o.", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("i.", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("PARTITION BY", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void PartitionBy_Ambiguity_UsesShortestSourcePrefix()
    {
        var aliasToSource = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["o"] = "TRADES",
            ["i"] = "QUOTES"
        };
        var sourceTypes = new[] { typeof(Quote), typeof(QuoteTable) };

        var buildAliasMetadata = typeof(KsqlCreateStatementBuilder)
            .GetMethod("BuildAliasMetadata", BindingFlags.NonPublic | BindingFlags.Static)!;
        var aliasMetadata = buildAliasMetadata.Invoke(null, new object[] { aliasToSource, sourceTypes });

        var selectClause = "o.Broker, i.Broker";
        var groupByClause = "GROUP BY o.Broker, i.Broker";
        var partitionClause = "o.Broker, i.Broker";
        var whereClause = "WHERE o.Broker = i.Broker";
        var havingClause = string.Empty;

        var dealias = typeof(KsqlCreateStatementBuilder)
            .GetMethod("DealiasClauses", BindingFlags.NonPublic | BindingFlags.Static)!;
        var parameters = new object?[] { aliasMetadata, selectClause, groupByClause, partitionClause, whereClause, havingClause };
        dealias.Invoke(null, parameters);

        partitionClause = (string)parameters[3]!;

        var dedup = typeof(KsqlCreateStatementBuilder)
            .GetMethod("DeduplicatePartitionColumns", BindingFlags.NonPublic | BindingFlags.Static)!;
        var result = (string)dedup.Invoke(null, new object?[] { partitionClause })!;

        Assert.Equal("QUOTES.Broker, TRADES.Broker", result);
    }
}
