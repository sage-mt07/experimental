using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Extensions;
using System;
using Xunit;

#nullable enable

namespace Kafka.Ksql.Linq.Tests.Core;

public class CoreExtensionsMoreTests
{
    private class Sample
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Ignored { get; set; }
        public int? Optional { get; set; }
    }

    private static EntityModel CreateModel() => new()
    {
        EntityType = typeof(Sample),
        TopicName = "t",
        AllProperties = typeof(Sample).GetProperties(),
        KeyProperties = new[] { typeof(Sample).GetProperty(nameof(Sample.Id))! }
    };

    [Fact]
    public void GetSerializableProperties_ReturnsAllProperties()
    {
        var model = CreateModel();
        var props = model.GetSerializableProperties();
        Assert.Contains(props, p => p.Name == nameof(Sample.Ignored));
        Assert.Contains(props, p => p.Name == nameof(Sample.Name));
    }

    [Fact]
    public void IsKafkaEntity_DetectsProperClass()
    {
        Assert.True(typeof(Sample).IsKafkaEntity());
        Assert.False(typeof(IDisposable).IsKafkaEntity());
    }

    [Fact]
    public void IsKafkaIgnored_ReturnsFalseWithoutAttribute()
    {
        var prop = typeof(Sample).GetProperty(nameof(Sample.Ignored))!;
        Assert.False(prop.IsKafkaIgnored());
    }

    [Fact]
    public void IsNullableProperty_WorksForNullableAndReference()
    {
        var opt = typeof(Sample).GetProperty(nameof(Sample.Optional))!;
        var name = typeof(Sample).GetProperty(nameof(Sample.Name))!;
        Assert.True(opt.IsNullableProperty());
        Assert.False(name.IsNullableProperty());
    }
}
