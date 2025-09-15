using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Extensions;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Core;

public class CoreExtensionsTests
{
    private class Sample
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Fact]
    public void GetTopicName_ReturnsAttributeValue()
    {
        var model = new EntityModel
        {
            EntityType = typeof(Sample),
            TopicName = "topic",
            KeyProperties = new[] { typeof(Sample).GetProperty(nameof(Sample.Id))! },
            AllProperties = typeof(Sample).GetProperties()
        };
        Assert.Equal("topic", model.GetTopicName());
        Assert.True(model.HasKeys());
        Assert.False(model.IsCompositeKey());
        var ordered = model.GetOrderedKeyProperties();
        Assert.Single(ordered);
    }

    [Fact]
    public void TypeExtension_ReturnsTopicName()
    {
        Assert.Equal("sample", typeof(Sample).GetKafkaTopicName());
        Assert.False(typeof(Sample).HasKafkaKeys());
    }

    [Fact]
    public void PropertyExtensions_DetectKey()
    {
        var prop = typeof(Sample).GetProperty(nameof(Sample.Id))!;
        Assert.False(prop.IsKafkaKey());
        Assert.Equal(0, prop.GetKeyOrder());
    }
}
