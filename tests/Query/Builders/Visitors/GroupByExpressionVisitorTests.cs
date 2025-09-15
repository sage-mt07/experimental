using Kafka.Ksql.Linq.Query.Builders;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Xunit;
using static Kafka.Ksql.Linq.Tests.PrivateAccessor;

namespace Kafka.Ksql.Linq.Tests.Query.Builders.Visitors;

public class GroupByExpressionVisitorTests
{
    [Fact]
    public void Visit_CompositeKey_ReturnsCommaSeparatedKeys()
    {
        Expression<Func<TestEntity, object>> expr = e => new { e.Id, e.Type };
        var visitor = new GroupByExpressionVisitor();
        visitor.Visit(expr.Body);
        var result = visitor.GetResult();
        Assert.Equal("test-topic.ID, Type", result);
    }

    [Fact]
    public void SimpleMember_ReturnsMemberName()
    {
        Expression<Func<Customer, object>> expr = e => e.Id;
        var visitor = new GroupByExpressionVisitor();
        visitor.Visit(expr.Body);
        var result = visitor.GetResult();
        Assert.Equal("Id", result);
    }

    private class Parent
    {
        public Child Child { get; set; } = new();
    }

    private class Child
    {
        public int Value { get; set; }
    }

    [Fact]
    public void Visit_NestedProperty_ReturnsLeafPropertyName()
    {
        Expression<Func<Parent, object>> expr = p => p.Child.Value;
        var visitor = new GroupByExpressionVisitor();
        visitor.Visit(expr.Body);
        var result = visitor.GetResult();
        Assert.Equal("Value", result);
    }

    private class CategoryEntity
    {
        public int Id { get; set; }
        public string Category { get; set; } = string.Empty;
        public string Type { get; set; } = string.Empty;
        public string Region { get; set; } = string.Empty;
        public int? NullableValue { get; set; }
        public List<int> List { get; set; } = new();
    }

    private static string Upper(string value) => value.ToUpperInvariant();

    private static string Left(string value, int length) => value;
    private static string Right(string value, int length) => value;

    private static MethodCallExpression GetCall<T>(Expression<Func<T, object>> expr)
    {
        Expression body = expr.Body;
        if (body is UnaryExpression unary && unary.NodeType == ExpressionType.Convert)
        {
            body = unary.Operand;
        }
        return (MethodCallExpression)body;
    }

    private class NumericEntity
    {
        public double Value { get; set; }
    }

    [Fact]
    public void AnonymousType_ReturnsCommaSeparated()
    {
        Expression<Func<Customer, object>> expr = e => new { e.Id, e.Region };
        var visitor = new GroupByExpressionVisitor();
        visitor.Visit(expr.Body);
        var result = visitor.GetResult();
        Assert.Equal("Id, Region", result);
    }

    [Fact]
    public void Visit_CompositeKeyWithCategory_ReturnsGroupByClause()
    {
        Expression<Func<CategoryEntity, object>> expr = e => new { e.Id, e.Category };
        var visitor = new GroupByExpressionVisitor();
        visitor.Visit(expr.Body);
        var result = visitor.GetResult();
        Assert.Equal("Id, Category", result);
    }

    [Fact]
    public void Visit_WithFunctionCall_TranslatesFunction()
    {
        Expression<Func<CategoryEntity, object>> expr = e => new { e.Id, Category = Upper(e.Category) };
        var visitor = new GroupByExpressionVisitor();
        visitor.Visit(expr.Body);
        var result = visitor.GetResult();
        Assert.Equal("Id, UPPER(Category)", result);
    }

    [Fact]
    public void NestedAnonymousType_FlattensProperties()
    {
        Expression<Func<CategoryEntity, object>> expr = e => new { e.Id, Sub = new { e.Region } };
        var visitor = new GroupByExpressionVisitor();
        visitor.Visit(expr.Body);
        var result = visitor.GetResult();
        Assert.Equal("Id, Region", result);
    }

    [Fact]
    public void NullCoalesce_ReturnsCoalesceFunction()
    {
        Expression<Func<CategoryEntity, object>> expr = e => e.NullableValue ?? 0;
        var visitor = new GroupByExpressionVisitor();
        visitor.Visit(expr.Body);
        var result = visitor.GetResult();
        Assert.Equal("COALESCE(NullableValue, 0)", result);
    }

    [Fact]
    public void Visit_ConstantExpression_Throws()
    {
        Expression<Func<CategoryEntity, object>> expr = e => new { Value = 1 };
        var visitor = new GroupByExpressionVisitor();
        Assert.Throws<InvalidOperationException>(() => visitor.Visit(expr.Body));
    }

    [Fact]
    public void Visit_UnsupportedExpression_Throws()
    {
        Expression<Func<CategoryEntity, object>> expr = e => e.List.First();
        var visitor = new GroupByExpressionVisitor();
        Assert.Throws<InvalidOperationException>(() => visitor.Visit(expr.Body));
    }

    [Fact]
    public void ProcessGroupByFunction_ToUpper_ReturnsFunctionCall()
    {
        Expression<Func<CategoryEntity, object>> expr = e => e.Category.ToUpper();
        var call = GetCall(expr);
        var visitor = new GroupByExpressionVisitor();
        var result = InvokePrivate<string>(visitor, "ProcessGroupByFunction", new[] { typeof(MethodCallExpression) }, args: new object[] { call });
        Assert.Equal("UPPER(Category)", result);
    }

    [Fact]
    public void ProcessGroupByFunction_ToString_ReturnsCast()
    {
        Expression<Func<CategoryEntity, object>> expr = e => e.Id.ToString();
        var call = GetCall(expr);
        var visitor = new GroupByExpressionVisitor();
        var result = InvokePrivate<string>(visitor, "ProcessGroupByFunction", new[] { typeof(MethodCallExpression) }, args: new object[] { call });
        Assert.Equal("CAST(Id AS VARCHAR)", result);
    }

    [Fact]
    public void ProcessGroupByFunction_Substring_ReturnsSubstring()
    {
        Expression<Func<CategoryEntity, object>> expr = e => e.Category.Substring(1, 2);
        var call = GetCall(expr);
        var visitor = new GroupByExpressionVisitor();
        var result = InvokePrivate<string>(visitor, "ProcessGroupByFunction", new[] { typeof(MethodCallExpression) }, args: new object[] { call });
        Assert.Equal("SUBSTRING(Category, 1, 2)", result);
    }

    [Fact]
    public void ProcessGroupByFunction_Left_ReturnsLeft()
    {
        Expression<Func<CategoryEntity, object>> expr = e => Left(e.Category, 3);
        var call = GetCall(expr);
        var visitor = new GroupByExpressionVisitor();
        var result = InvokePrivate<string>(visitor, "ProcessGroupByFunction", new[] { typeof(MethodCallExpression) }, args: new object[] { call });
        Assert.Equal("LEFT(Category, 3)", result);
    }

    [Fact]
    public void ProcessGroupByFunction_Right_ReturnsRight()
    {
        Expression<Func<CategoryEntity, object>> expr = e => Right(e.Category, 4);
        var call = GetCall(expr);
        var visitor = new GroupByExpressionVisitor();
        var result = InvokePrivate<string>(visitor, "ProcessGroupByFunction", new[] { typeof(MethodCallExpression) }, args: new object[] { call });
        Assert.Equal("RIGHT(Category, 4)", result);
    }

    [Fact]
    public void ProcessGroupByFunction_Round_ReturnsRound()
    {
        Expression<Func<NumericEntity, object>> expr = e => Math.Round(e.Value, 2);
        var call = GetCall(expr);
        var visitor = new GroupByExpressionVisitor();
        var result = InvokePrivate<string>(visitor, "ProcessGroupByFunction", new[] { typeof(MethodCallExpression) }, args: new object[] { call });
        Assert.Equal("ROUND(Value, 2)", result);
    }

    [Fact]
    public void ProcessGroupByFunction_Floor_ReturnsFloor()
    {
        Expression<Func<NumericEntity, object>> expr = e => Math.Floor(e.Value);
        var call = GetCall(expr);
        var visitor = new GroupByExpressionVisitor();
        var result = InvokePrivate<string>(visitor, "ProcessGroupByFunction", new[] { typeof(MethodCallExpression) }, args: new object[] { call });
        Assert.Equal("FLOOR(Value)", result);
    }

    [Fact]
    public void BinaryExpression_ReturnsExpression()
    {
        Expression<Func<NumericEntity, object>> expr = e => e.Value + 1;
        var visitor = new GroupByExpressionVisitor();
        visitor.Visit(expr.Body);
        var result = visitor.GetResult();
        Assert.Equal("Value + 1", result);
    }
}
