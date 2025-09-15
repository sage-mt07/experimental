using Kafka.Ksql.Linq.Query.Pipeline;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query;

public class ValidationResultTests
{
    [Fact]
    public void Failure_CreatesInvalidResult()
    {
        var result = ValidationResult.Failure("a", "b");
        Assert.False(result.IsValid);
        Assert.Equal(2, result.Errors.Count);
        Assert.Equal("a; b", result.GetErrorMessage());
    }

    [Fact]
    public void Success_HasNoErrors()
    {
        var result = ValidationResult.Success;
        Assert.True(result.IsValid);
        Assert.Empty(result.Errors);
        Assert.Equal(string.Empty, result.GetErrorMessage());
    }
}
