using Kafka.Ksql.Linq.Application;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Application;

public class KsqlContextOptionsExtensionsTests
{
    [Fact]
    public void UseSchemaRegistry_WithUrl_ConfiguresClient()
    {
        var options = new KsqlContextOptions();
        options.UseSchemaRegistry("http://localhost:8088");
        Assert.NotNull(options.SchemaRegistryClient);
    }

    [Fact]
    public void UseSchemaRegistry_WithConfig_ConfiguresClient()
    {
        var options = new KsqlContextOptions();
        var config = new Confluent.SchemaRegistry.SchemaRegistryConfig { Url = "u" };
        options.UseSchemaRegistry(config);
        Assert.NotNull(options.SchemaRegistryClient);
    }

    [Fact]
    public void EnableLogging_SetsLoggerFactory()
    {
        var options = new KsqlContextOptions();
        var factory = NullLoggerFactory.Instance;
        options.EnableLogging(factory);
        Assert.Equal(factory, options.LoggerFactory);
    }

    [Fact]
    public void ConfigureValidation_UpdatesFlags()
    {
        var options = new KsqlContextOptions();
        options.ConfigureValidation(autoRegister: false, failOnErrors: false, enablePreWarming: false);
        Assert.False(options.AutoRegisterSchemas);
        Assert.False(options.FailOnInitializationErrors);
        Assert.False(options.EnableCachePreWarming);
    }

    [Fact]
    public void WithTimeouts_SetsTimeout()
    {
        var options = new KsqlContextOptions();
        var ts = System.TimeSpan.FromSeconds(5);
        options.WithTimeouts(ts);
        Assert.Equal(ts, options.SchemaRegistrationTimeout);
    }

}
