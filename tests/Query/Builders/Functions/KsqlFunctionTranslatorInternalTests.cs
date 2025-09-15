using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Query.Builders.Functions;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Xunit;
using static Kafka.Ksql.Linq.Tests.PrivateAccessor;

namespace Kafka.Ksql.Linq.Tests.Query.Builders.Functions;

public class KsqlFunctionTranslatorInternalTests
{
    private enum SampleEnum { A }
    private class CustomType { }

    public static IEnumerable<object[]> MapToKsqlTypeData()
    {
        yield return new object[] { typeof(string), "VARCHAR" };
        yield return new object[] { typeof(int), "INTEGER" };
        yield return new object[] { typeof(long), "BIGINT" };
        yield return new object[] { typeof(float), "DOUBLE" };
        yield return new object[] { typeof(double), "DOUBLE" };
        yield return new object[] { typeof(bool), "BOOLEAN" };
        yield return new object[] { typeof(DateTime), "TIMESTAMP" };
        yield return new object[] { typeof(Guid), "VARCHAR" };
        yield return new object[] { typeof(decimal), $"DECIMAL({DecimalPrecisionConfig.DecimalPrecision}, {DecimalPrecisionConfig.DecimalScale})" };
        yield return new object[] { typeof(byte[]), "BYTES" };
    }

    [Theory]
    [MemberData(nameof(MapToKsqlTypeData))]
    public void MapToKsqlType_ReturnsExpected(Type type, string expected)
    {
        var result = InvokePrivate<string>(typeof(KsqlFunctionTranslator), "MapToKsqlType", new[] { typeof(Type) }, null, type);
        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData(typeof(SampleEnum))]
    [InlineData(typeof(CustomType))]
    public void MapToKsqlType_Unsupported_Throws(Type type)
    {
        Assert.Throws<NotSupportedException>(() =>
            InvokePrivate<string>(typeof(KsqlFunctionTranslator), "MapToKsqlType", new[] { typeof(Type) }, null, type));
    }

    [Theory]
    [InlineData("SUM", "DOUBLE")]
    [InlineData("sum", "DOUBLE")]
    [InlineData("SuM", "DOUBLE")]
    [InlineData("AVG", "DOUBLE")]
    [InlineData("avg", "DOUBLE")]
    [InlineData("COUNT", "BIGINT")]
    [InlineData("count", "BIGINT")]
    [InlineData("MAX", "ANY")]
    [InlineData("MiN", "ANY")]
    [InlineData("TOPK", "ARRAY")]
    [InlineData("Histogram", "MAP")]
    [InlineData("unknown", "UNKNOWN")]
    public void InferTypeFromMethodName_ReturnsExpected(string name, string expected)
    {
        var result = InvokePrivate<string>(typeof(KsqlFunctionTranslator), "InferTypeFromMethodName", new[] { typeof(string) }, null, name);
        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData(ExpressionType.Add, "+")]
    [InlineData(ExpressionType.Subtract, "-")]
    [InlineData(ExpressionType.Multiply, "*")]
    [InlineData(ExpressionType.Divide, "/")]
    [InlineData(ExpressionType.Equal, "=")]
    [InlineData(ExpressionType.NotEqual, "!=")]
    [InlineData(ExpressionType.GreaterThan, ">")]
    [InlineData(ExpressionType.LessThanOrEqual, "<=")]
    public void GetOperator_ReturnsExpected(ExpressionType nodeType, string expected)
    {
        var result = InvokePrivate<string>(typeof(KsqlFunctionTranslator), "GetOperator", new[] { typeof(ExpressionType) }, null, nodeType);
        Assert.Equal(expected, result);
    }

    [Fact]
    public void GetOperator_Unknown_Throws()
    {
        Assert.Throws<NotSupportedException>(() =>
            InvokePrivate<string>(typeof(KsqlFunctionTranslator), "GetOperator", new[] { typeof(ExpressionType) }, null, ExpressionType.Throw));
    }
}
