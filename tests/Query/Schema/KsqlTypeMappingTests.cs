using Kafka.Ksql.Linq.Query.Schema;
using System;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.KsqlSchema;

public class KsqlTypeMappingTests
{
    [Fact]
    public void DictionaryStringString_ReturnsMap()
    {
        var result = KsqlTypeMapping.MapToKsqlType(typeof(Dictionary<string, string>), null);
        Assert.Equal("MAP<STRING, STRING>", result);
    }

    [Fact]
    public void NonStringKey_Throws()
    {
        var ex = Assert.Throws<NotSupportedException>(() => KsqlTypeMapping.MapToKsqlType(typeof(Dictionary<int, string>), null));
        Assert.Equal("ksqlDB MAP key must be STRING.", ex.Message);
    }

    [Fact]
    public void NonStringValue_Throws()
    {
        var ex = Assert.Throws<NotSupportedException>(() => KsqlTypeMapping.MapToKsqlType(typeof(Dictionary<string, int>), null));
        Assert.Equal("Only Dictionary<string, string> is supported currently.", ex.Message);
    }
}
