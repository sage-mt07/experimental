using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Core.Modeling;
using System;
using System.Linq.Expressions;
using Xunit;

namespace Kafka.Ksql.Linq.Tests;

public class ApplyModelBuilderSettingsTests
{
    [KsqlTable]
    private class Sample
    {
        public int Id { get; set; }
        public DateTime Time { get; set; }
    }

    private static EntityModel BuildModel()
    {
        var modelBuilder = new ModelBuilder();
        var builder = modelBuilder.Entity<Sample>();
        builder.OnError(ErrorAction.DLQ);
        var model = builder.GetModel();
        model.EnableCache = false;
        model.DeserializationErrorPolicy = DeserializationErrorPolicy.DLQ;
        model.BarTimeSelector = (Expression<Func<Sample, DateTime>>)(x => x.Time);
        return model;
    }

    [Fact]
    public void AutoRegisteredModel_PropertiesFromModelBuilderAreApplied()
    {
        var model = BuildModel();

        Assert.Equal(ErrorAction.DLQ, model.ErrorAction);
        Assert.Equal(DeserializationErrorPolicy.DLQ, model.DeserializationErrorPolicy);
        Assert.False(model.EnableCache);
        Assert.NotNull(model.BarTimeSelector);
    }
}
