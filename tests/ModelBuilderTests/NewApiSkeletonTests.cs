using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Core.Modeling;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.ModelBuilderTests;

public class NewApiSkeletonTests
{
    [KsqlTopic("sample")]
    private class Sample
    {
        [KsqlKey(Order = 0)]
        public int Id { get; set; }
    }

    [Fact]
    public void Attributes_ConfiguresModel()
    {
        var builder = new ModelBuilder();
        builder.Entity<Sample>();

        var model = builder.GetEntityModel<Sample>();
        Assert.NotNull(model);
        Assert.Equal("sample", model!.TopicName);
        Assert.Single(model.KeyProperties);
    }
}
