using System;
using System.Reflection;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Builders;

public class KsqlCreateWindowedStatementBuilderInternalTests
{
    [Fact]
    public void InjectWindowAfterFrom_Inserts_AfterAlias_BeforeJoin()
    {
        var baseSql = "CREATE TABLE x WITH (KAFKA_TOPIC='x', KEY_FORMAT='AVRO', VALUE_FORMAT='AVRO') AS\n" +
                      "SELECT col\n" +
                      "FROM LEFTSRC o JOIN RIGHTSRC i WITHIN 300 SECONDS ON (o.Id = i.Id)\n" +
                      "GROUP BY col\n" +
                      "EMIT FINAL;";
        var window = "WINDOW TUMBLING (SIZE 1 MINUTES)";

        var type = Type.GetType("Kafka.Ksql.Linq.Query.Builders.KsqlCreateWindowedStatementBuilder, Kafka.Ksql.Linq");
        Assert.NotNull(type);
        var method = type!.GetMethod("InjectWindowAfterFrom", BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(method);
        var result = (string)method!.Invoke(null, new object[] { baseSql, window })!;

        Kafka.Ksql.Linq.Tests.Utils.SqlAssert.AssertOrderNormalized(
            result,
            "from leftsrc o window tumbling",
            "join rightsrc i");
    }
}

