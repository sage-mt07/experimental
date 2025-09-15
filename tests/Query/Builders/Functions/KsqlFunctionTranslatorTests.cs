using Kafka.Ksql.Linq.Query.Builders.Functions;
using System;
using System.Linq.Expressions;
using Xunit;
using static Kafka.Ksql.Linq.Tests.PrivateAccessor;

namespace Kafka.Ksql.Linq.Tests.Query.Builders.Functions;

public class KsqlFunctionTranslatorTests
{
    private class Entity
    {
        public string Name { get; set; } = string.Empty;
        public string Description { get; set; } = string.Empty;
        public decimal Amount { get; set; }
        public DateTime Date { get; set; }
        public string Comment { get; set; } = string.Empty;
        public int Value { get; set; }
    }

    private static int Length(string value) => value.Length;
    private static string Substring(string value) => value;
    private static int UnknownFunc(int value) => value;
    private static int Sum(int value) => value;
    private static int Count() => 0;

    private static MethodCallExpression GetCall(Expression<Func<Entity, object>> expr)
    {
        Expression body = expr.Body;
        if (body is UnaryExpression unary && unary.NodeType == ExpressionType.Convert)
        {
            body = unary.Operand;
        }
        return (MethodCallExpression)body;
    }

    [Fact]
    public void Translate_ToUpper_ReturnsUpper()
    {
        Expression<Func<Entity, object>> expr = e => e.Name.ToUpper();
        var call = GetCall(expr);
        var result = KsqlFunctionTranslator.TranslateMethodCall(call);
        Assert.Equal("UPPER(Name)", result);
    }

    [Fact]
    public void Translate_Length_ReturnsLen()
    {
        Expression<Func<Entity, object>> expr = e => Length(e.Description);
        var call = GetCall(expr);
        var result = KsqlFunctionTranslator.TranslateMethodCall(call);
        Assert.Equal("LEN(Description)", result);
    }

    [Fact]
    public void Translate_ToString_ReturnsCast()
    {
        Expression<Func<Entity, object>> expr = e => e.Amount.ToString();
        var call = GetCall(expr);
        var result = KsqlFunctionTranslator.TranslateMethodCall(call);
        Assert.Equal("CAST(Amount AS VARCHAR)", result);
    }

    [Fact]
    public void Translate_AddDays_ReturnsDateadd()
    {
        Expression<Func<Entity, object>> expr = e => e.Date.AddDays(1);
        var call = GetCall(expr);
        var result = KsqlFunctionTranslator.TranslateMethodCall(call);
        Assert.Equal("DATEADD('day', 1, Date)", result);
    }

    [Fact]
    public void Translate_Sum_ReturnsSum()
    {
        Expression<Func<Entity, object>> expr = e => Sum(e.Value);
        var call = GetCall(expr);
        var result = KsqlFunctionTranslator.TranslateMethodCall(call);
        Assert.Equal("SUM(Value)", result);
    }

    [Fact]
    public void Translate_Count_NoArgs_ReturnsCountStar()
    {
        Expression<Func<Entity, object>> expr = _ => Count();
        var call = GetCall(expr);
        var result = KsqlFunctionTranslator.TranslateMethodCall(call);
        Assert.Equal("COUNT(*)", result);
    }

    [Fact]
    public void Translate_Substring_ReturnsSubstring()
    {
        Expression<Func<Entity, object>> expr = e => e.Name.Substring(1, 2);
        var call = GetCall(expr);
        var result = KsqlFunctionTranslator.TranslateMethodCall(call);
        Assert.Equal("SUBSTRING(Name, 1, 2)", result);
    }

    [Fact]
    public void Translate_NestedFunctions_ReturnsNestedString()
    {
        Expression<Func<Entity, object>> expr = e => e.Comment.ToLower().Trim();
        var call = GetCall(expr);
        var result = KsqlFunctionTranslator.TranslateMethodCall(call);
        Assert.Equal("TRIM(LOWER(Comment))", result);
    }

    [Fact]
    public void Translate_UnsupportedFunction_Throws()
    {
        Expression<Func<Entity, object>> expr = e => UnknownFunc(e.Value);
        var call = GetCall(expr);
        Assert.Throws<NotSupportedException>(() => KsqlFunctionTranslator.TranslateMethodCall(call));
    }

    [Fact]
    public void Translate_InvalidArgumentCount_Throws()
    {
        Expression<Func<Entity, object>> expr = e => Substring(e.Name);
        var call = GetCall(expr);
        var ex = Assert.Throws<ArgumentException>(() => KsqlFunctionTranslator.TranslateMethodCall(call));
        Assert.Contains("Substring", ex.Message);
    }

    private static string ToLower(int value) => value.ToString().ToLower();
    private static int Length(int value) => value.ToString().Length;
    private static decimal Sum(decimal value) => value;

    [Fact]
    public void Translate_ToLower_WithInt_Throws()
    {
        Expression<Func<Entity, object>> expr = e => ToLower(e.Value);
        var call = GetCall(expr);
        Assert.Throws<NotSupportedException>(() => KsqlFunctionTranslator.TranslateMethodCall(call));
    }

    [Fact]
    public void Translate_Length_WithInt_Throws()
    {
        Expression<Func<Entity, object>> expr = e => Length(e.Value);
        var call = GetCall(expr);
        Assert.Throws<NotSupportedException>(() => KsqlFunctionTranslator.TranslateMethodCall(call));
    }

    [Fact]
    public void Translate_Sum_WithDecimal_Throws()
    {
        Expression<Func<Entity, object>> expr = e => Sum(e.Amount);
        var call = GetCall(expr);
        Assert.Throws<NotSupportedException>(() => KsqlFunctionTranslator.TranslateMethodCall(call));
    }

    [Fact]
    public void GetSqlOperator_Add_ReturnsPlus()
    {
        var result = InvokePrivate<string>(typeof(KsqlFunctionTranslator), "GetOperator", new[] { typeof(ExpressionType) }, null, ExpressionType.Add);
        Assert.Equal("+", result);
    }

    [Fact]
    public void GetSqlOperator_Subtract_ReturnsMinus()
    {
        var result = InvokePrivate<string>(typeof(KsqlFunctionTranslator), "GetOperator", new[] { typeof(ExpressionType) }, null, ExpressionType.Subtract);
        Assert.Equal("-", result);
    }

    [Fact]
    public void GetSqlOperator_Multiply_ReturnsAsterisk()
    {
        var result = InvokePrivate<string>(typeof(KsqlFunctionTranslator), "GetOperator", new[] { typeof(ExpressionType) }, null, ExpressionType.Multiply);
        Assert.Equal("*", result);
    }

    [Fact]
    public void GetSqlOperator_Divide_ReturnsSlash()
    {
        var result = InvokePrivate<string>(typeof(KsqlFunctionTranslator), "GetOperator", new[] { typeof(ExpressionType) }, null, ExpressionType.Divide);
        Assert.Equal("/", result);
    }

    [Fact]
    public void GetSqlOperator_Equal_ReturnsEquals()
    {
        var result = InvokePrivate<string>(typeof(KsqlFunctionTranslator), "GetOperator", new[] { typeof(ExpressionType) }, null, ExpressionType.Equal);
        Assert.Equal("=", result);
    }

    [Fact]
    public void GetSqlOperator_NotEqual_ReturnsNotEquals()
    {
        var result = InvokePrivate<string>(typeof(KsqlFunctionTranslator), "GetOperator", new[] { typeof(ExpressionType) }, null, ExpressionType.NotEqual);
        Assert.Equal("!=", result);
    }

    [Fact]
    public void GetSqlOperator_GreaterThan_ReturnsGreaterThan()
    {
        var result = InvokePrivate<string>(typeof(KsqlFunctionTranslator), "GetOperator", new[] { typeof(ExpressionType) }, null, ExpressionType.GreaterThan);
        Assert.Equal(">", result);
    }

    [Fact]
    public void GetSqlOperator_LessThanOrEqual_ReturnsLessThanEqual()
    {
        var result = InvokePrivate<string>(typeof(KsqlFunctionTranslator), "GetOperator", new[] { typeof(ExpressionType) }, null, ExpressionType.LessThanOrEqual);
        Assert.Equal("<=", result);
    }

    [Fact]
    public void GetSqlOperator_Unsupported_ThrowsNotSupported()
    {
        Assert.Throws<NotSupportedException>(() => InvokePrivate<string>(typeof(KsqlFunctionTranslator), "GetOperator", new[] { typeof(ExpressionType) }, null, ExpressionType.Coalesce));
    }
}
