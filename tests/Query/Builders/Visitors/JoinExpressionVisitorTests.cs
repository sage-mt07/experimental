using Kafka.Ksql.Linq.Query.Builders;
using System;
using System.Linq.Expressions;
using Xunit;
using static Kafka.Ksql.Linq.Tests.PrivateAccessor;

namespace Kafka.Ksql.Linq.Tests.Query.Builders.Visitors;

public class JoinExpressionVisitorTests
{
    [Theory]
    [InlineData(ExpressionType.Equal, "=")]
    [InlineData(ExpressionType.NotEqual, "!=")]
    [InlineData(ExpressionType.GreaterThan, ">")]
    [InlineData(ExpressionType.GreaterThanOrEqual, ">=")]
    [InlineData(ExpressionType.LessThan, "<")]
    [InlineData(ExpressionType.LessThanOrEqual, "<=")]
    public void GetOperator_SupportedTypes_ReturnsExpected(ExpressionType type, string expected)
    {
        var result = InvokePrivate<string>(typeof(JoinExpressionVisitor), "GetOperator", new[] { typeof(ExpressionType) }, null, type);
        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData(ExpressionType.Add)]
    [InlineData(ExpressionType.Coalesce)]
    public void GetOperator_UnsupportedTypes_Throws(ExpressionType type)
    {
        Assert.Throws<NotSupportedException>(() => InvokePrivate<string>(typeof(JoinExpressionVisitor), "GetOperator", new[] { typeof(ExpressionType) }, null, type));
    }
}
