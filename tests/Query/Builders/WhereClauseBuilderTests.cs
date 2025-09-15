using Kafka.Ksql.Linq.Query.Builders;
using System;
using System.Linq.Expressions;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Builders;

public class WhereClauseBuilderTests
{
    [Fact]
    public void BuildCondition_Negation_ReturnsEqualsFalse()
    {
        Expression<Func<TestEntity, bool>> expr = e => !e.IsActive;
        var builder = new WhereClauseBuilder();
        var sql = builder.BuildCondition(expr.Body);
        Assert.Equal("(IsActive = false)", sql);
    }

    [Fact]
    public void BuildCondition_NullComparison_ReturnsIsNull()
    {
        Expression<Func<TestEntity, bool>> expr = e => e.IsProcessed == null;
        var builder = new WhereClauseBuilder();
        var sql = builder.BuildCondition(expr.Body);
        Assert.Equal("IsProcessed IS NULL", sql);
    }

    [Fact]
    public void BuildCondition_NotNullComparison_ReturnsIsNotNull()
    {
        Expression<Func<TestEntity, bool>> expr = e => e.IsProcessed != null;
        var builder = new WhereClauseBuilder();
        var sql = builder.BuildCondition(expr.Body);
        Assert.Equal("IsProcessed IS NOT NULL", sql);
    }

    [Fact]
    public void BuildCondition_AndCondition_ReturnsJoinedClause()
    {
        Expression<Func<TestEntity, bool>> expr = e => e.Id == 1 && e.Name == "foo";
        var builder = new WhereClauseBuilder();
        var sql = builder.BuildCondition(expr.Body);
        Assert.Equal("((Id = 1) AND (Name = 'foo'))", sql);
    }

    [Fact]
    public void BuildCondition_MultipleAndConditions_ReturnsJoinedClauses()
    {
        Expression<Func<TestEntity, bool>> expr = e => e.Id == 1 && e.Name == "foo" && e.Type == "bar";
        var builder = new WhereClauseBuilder();
        var sql = builder.BuildCondition(expr.Body);
        Assert.Equal("(((Id = 1) AND (Name = 'foo')) AND (Type = 'bar'))", sql);
    }

    private class Order
    {
        public int Amount { get; set; }
        public int? CustomerId { get; set; }
    }

    [Theory]
    [InlineData("==", 100, "(Amount = 100)")]
    [InlineData("!=", 100, "(Amount != 100)")]
    [InlineData("<", 100, "(Amount < 100)")]
    [InlineData("<=", 100, "(Amount <= 100)")]
    [InlineData(">", 100, "(Amount > 100)")]
    [InlineData(">=", 100, "(Amount >= 100)")]
    public void BuildCondition_ComparisonOperators(string op, int value, string expected)
    {
        var parameter = Expression.Parameter(typeof(Order), "o");
        var left = Expression.Property(parameter, "Amount");
        var right = Expression.Constant(value);

        Expression body = op switch
        {
            "==" => Expression.Equal(left, right),
            "!=" => Expression.NotEqual(left, right),
            "<" => Expression.LessThan(left, right),
            "<=" => Expression.LessThanOrEqual(left, right),
            ">" => Expression.GreaterThan(left, right),
            ">=" => Expression.GreaterThanOrEqual(left, right),
            _ => throw new NotSupportedException()
        };

        var builder = new WhereClauseBuilder();
        var sql = builder.BuildCondition(body);

        Assert.Equal(expected, sql);
    }

    [Fact]
    public void BuildCondition_IsNullComparison_ReturnsIsNull()
    {
        Expression<Func<Order, bool>> expr = o => o.CustomerId == null;
        var builder = new WhereClauseBuilder();
        var sql = builder.BuildCondition(expr.Body);
        Assert.Equal("CustomerId IS NULL", sql);
    }

    [Fact]
    public void BuildCondition_IsNotNullComparison_ReturnsIsNotNull()
    {
        Expression<Func<Order, bool>> expr = o => o.CustomerId != null;
        var builder = new WhereClauseBuilder();
        var sql = builder.BuildCondition(expr.Body);
        Assert.Equal("CustomerId IS NOT NULL", sql);
    }
}
