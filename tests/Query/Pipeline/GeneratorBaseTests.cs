using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Query.Pipeline;
using System;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Pipeline;

public class GeneratorBaseTests
{
    [Fact]
    public void AssembleQuery_OrdersPartsAndTrims()
    {
        var select = QueryPart.Required("SELECT *", 10);
        var from = QueryPart.Required("FROM t", 20);
        var where = QueryPart.Required("WHERE Id = 1", 40);

        var result = PrivateAccessor.InvokePrivate<string>(
            typeof(GeneratorBase),
            "AssembleQuery",
            new[] { typeof(QueryPart[]) },
            args: new object[] { new[] { where, select, from } });

        Assert.Equal("SELECT * FROM t WHERE Id = 1", result);
    }

    [Fact]
    public void AssembleQuery_IgnoresEmptyOptionalParts()
    {
        var select = QueryPart.Required("SELECT *", 10);
        var emptyOpt = QueryPart.Optional(string.Empty, 30);
        var from = QueryPart.Required("FROM t", 20);

        var result = PrivateAccessor.InvokePrivate<string>(
            typeof(GeneratorBase),
            "AssembleQuery",
            new[] { typeof(QueryPart[]) },
            args: new object[] { new[] { select, emptyOpt, from } });

        Assert.Equal("SELECT * FROM t", result);
    }

    [Fact]
    public void AssembleQuery_NoValidParts_Throws()
    {
        Assert.ThrowsAny<Exception>(() =>
            PrivateAccessor.InvokePrivate<string>(
                typeof(GeneratorBase),
                "AssembleQuery",
                new[] { typeof(QueryPart[]) },
                args: new object[] { new[] { QueryPart.Optional(string.Empty) } }));
    }

    public static IEnumerable<object[]> MapToKSqlTypeData()
    {
        yield return new object[] { typeof(string), "VARCHAR" };
        yield return new object[] { typeof(int), "INTEGER" };
        yield return new object[] { typeof(long), "BIGINT" };
        yield return new object[] { typeof(double), "DOUBLE" };
        yield return new object[] { typeof(bool), "BOOLEAN" };
        yield return new object[] { typeof(DateTime), "TIMESTAMP" };
        yield return new object[] { typeof(decimal), $"DECIMAL({DecimalPrecisionConfig.DecimalPrecision}, {DecimalPrecisionConfig.DecimalScale})" };
        yield return new object[] { typeof(byte[]), "BYTES" };
    }

    [Theory]
    [MemberData(nameof(MapToKSqlTypeData))]
    public void MapToKSqlType_ReturnsExpected(Type type, string expected)
    {
        var result = PrivateAccessor.InvokePrivate<string>(
            typeof(GeneratorBase),
            "MapToKSqlType",
            new[] { typeof(Type) },
            args: new object[] { type });

        Assert.Equal(expected, result);
    }

    [Fact]
    public void MapToKSqlType_Unknown_ThrowsNotSupported()
    {
        Assert.Throws<NotSupportedException>(() =>
            PrivateAccessor.InvokePrivate<string>(
                typeof(GeneratorBase),
                "MapToKSqlType",
                new[] { typeof(Type) },
                args: new object[] { typeof(Uri) }));
    }
}
