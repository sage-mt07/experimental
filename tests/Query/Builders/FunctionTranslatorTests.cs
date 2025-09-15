using Kafka.Ksql.Linq.Query.Builders.Functions;
using System;
using System.Linq.Expressions;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Builders;

public class FunctionTranslatorTests
{
    [Fact]
    public void TranslateMethodCall_ToUpper_ReturnsUpperFunction()
    {
        var method = typeof(string).GetMethod(nameof(string.ToUpper), Type.EmptyTypes)!;
        var call = Expression.Call(Expression.Constant("a"), method);
        var result = KsqlFunctionTranslator.TranslateMethodCall(call);
        Assert.Equal("UPPER('a')", result);
    }

    [Fact]
    public void TranslateMethodCall_ConvertToInt_ReturnsCastExpression()
    {
        var method = typeof(Convert).GetMethod(nameof(Convert.ToInt32), [typeof(string)])!;
        var call = Expression.Call(method, Expression.Constant("1"));
        var result = KsqlFunctionTranslator.TranslateMethodCall(call);
        Assert.Equal("CAST('1' AS INTEGER)", result);
    }

    [Fact]
    public void TranslateMethodCall_UnknownMethod_Throws()
    {
        var method = typeof(FunctionTranslatorTests).GetMethod(nameof(Custom), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        var call = Expression.Call(method);
        Assert.Throws<NotSupportedException>(() => KsqlFunctionTranslator.TranslateMethodCall(call));
    }

    private static void Custom() { }
}
