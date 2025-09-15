using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Modeling;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Core;

public class EntityModelBuilderTests
{
    private class Sample { public int Id { get; set; } }

    [Fact]
    public void Constructor_StoresModel()
    {
        var model = new EntityModel { EntityType = typeof(Sample), AllProperties = typeof(Sample).GetProperties(), KeyProperties = new[] { typeof(Sample).GetProperty(nameof(Sample.Id))! } };
        var builder = new EntityModelBuilder<Sample>(model, new ModelBuilder());
        Assert.Equal(model, builder.GetModel());
        var str = builder.ToString();
        Assert.Contains("Sample", str);
    }

    [Fact]
    public void ObsoleteMethods_ThrowViaReflection()
    {
        var model = new EntityModel { EntityType = typeof(Sample), AllProperties = typeof(Sample).GetProperties(), KeyProperties = new[] { typeof(Sample).GetProperty(nameof(Sample.Id))! } };
        var builder = new EntityModelBuilder<Sample>(model, new ModelBuilder());
        Assert.ThrowsAny<System.Exception>(() => PrivateAccessor.InvokePrivate(builder, "HasTopicName", new[] { typeof(string) }, args: new object[] { "t" }));
    }
}
