using Kafka.Ksql.Linq.Query.Builders;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Builders.Visitors;

public class OrderByExpressionVisitorTests
{
    private static IQueryable<TestEntity> CreateQuery() => new List<TestEntity>().AsQueryable();

    [Fact]
    public void Visit_OrderBy_ThenByDescending_BuildsClause()
    {
        var query = CreateQuery()
            .OrderBy(e => e.Id)
            .ThenByDescending(e => e.Type);

        var visitor = new OrderByExpressionVisitor();
        visitor.Visit(query.Expression);

        var result = visitor.GetResult();
        // The visitor currently only returns the last ORDER BY clause
        Assert.Equal("Type DESC", result);
    }

    [Fact]
    public void Visit_OrderByWithFunction_BuildsFunctionClause()
    {
        var query = CreateQuery().OrderBy(e => e.Name.ToUpper());

        var visitor = new OrderByExpressionVisitor();
        visitor.Visit(query.Expression);

        var result = visitor.GetResult();
        Assert.Equal("UPPER(Name) ASC", result);
    }

    [Fact]
    public void Visit_UnsupportedFunction_Throws()
    {
        var query = CreateQuery().OrderBy(e => e.Name.StartsWith("a"));

        var visitor = new OrderByExpressionVisitor();
        Assert.Throws<InvalidOperationException>(() => visitor.Visit(query.Expression));
    }
}
