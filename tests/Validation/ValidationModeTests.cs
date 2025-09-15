using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Modeling;
using System;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Validation;

public class ValidationModeTests
{
    private abstract class AbstractOrder
    {
        public int Id { get; set; }
    }

    [Fact]
    public void RelaxedMode_ShouldReturnFalse_OnInvalidModel()
    {
        var builder = new ModelBuilder(ValidationMode.Relaxed);
        builder.AddEntityModel<AbstractOrder>();

        var result = builder.ValidateAllModels();

        Assert.False(result);
    }

    [Fact]
    public void StrictMode_ShouldThrow_OnInvalidModel()
    {
        var builder = new ModelBuilder(ValidationMode.Strict);
        builder.AddEntityModel<AbstractOrder>();

        var ex = Assert.Throws<InvalidOperationException>(() => builder.ValidateAllModels());
        Assert.Contains("Entity model validation failed", ex.Message);
    }
}
