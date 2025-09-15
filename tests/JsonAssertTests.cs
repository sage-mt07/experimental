using Xunit;

namespace Kafka.Ksql.Linq.Tests;

public class JsonAssertTests
{
    [Fact]
    public void Equal_IgnoresFormattingAndCase()
    {
        var expected = "{\n  \"Name\": \"Alice\", \"Age\": 30 }";
        var actual = "{ \"name\":\n\"alice\",\n\"age\":30 }";
        JsonAssert.Equal(expected, actual);
    }
}
