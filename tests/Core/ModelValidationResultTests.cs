using Kafka.Ksql.Linq.Core.Modeling;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Core;

public class ModelValidationResultTests
{
    [Fact]
    public void Properties_DefaultValues()
    {
        var r = new ModelValidationResult();
        Assert.False(r.HasErrors);
        Assert.True(r.IsValid);
        Assert.Empty(r.EntityErrors);
        Assert.Empty(r.EntityWarnings);
    }

    [Fact]
    public void GetSummary_FormatsMessages()
    {
        var r = new ModelValidationResult
        {
            HasErrors = true,
            EntityErrors = new() { [typeof(string)] = new() { "e1" } },
            EntityWarnings = new() { [typeof(int)] = new() { "w1" } }
        };
        var sum = r.GetSummary();
        Assert.Contains("Model validation failed", sum);
        Assert.Contains("w1", sum);
    }
}
