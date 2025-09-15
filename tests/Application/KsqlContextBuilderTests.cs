using Kafka.Ksql.Linq.Application;
using Kafka.Ksql.Linq.Configuration;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Application;

// Dummy context used for KsqlContextBuilder tests
public class DummyContext : KsqlContext
{
    public DummyContext() : base(new KsqlDslOptions()) { }
    public DummyContext(KsqlDslOptions options) : base(options) { }

    // Skip heavy initialization during tests
    protected override bool SkipSchemaRegistration => true;
}

public class KsqlContextBuilderTests
{
    [Fact]
    public void Create_ReturnsBuilder()
    {
        var builder = KsqlContextBuilder.Create();
        Assert.NotNull(builder);
    }

    [Fact]
    public void BuildContext_CreatesInstance()
    {
        var ctx = KsqlContextBuilder.Create().UseSchemaRegistry("u").BuildContext<DummyContext>();
        Assert.IsType<DummyContext>(ctx);
    }

    [Fact]
    public void Builder_Methods_ConfigureOptions()
    {
        var factory = Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory.Instance;
        var builder = KsqlContextBuilder.Create()
            .UseSchemaRegistry("http://localhost:8088")
            .EnableLogging(factory)
            .ConfigureValidation(autoRegister: false, failOnErrors: false, enablePreWarming: false)
            .WithTimeouts(System.TimeSpan.FromSeconds(5));
        var options = builder.Build();
        Assert.NotNull(options.SchemaRegistryClient);
        Assert.Equal(factory, options.LoggerFactory);
        Assert.False(options.AutoRegisterSchemas);
        Assert.False(options.FailOnInitializationErrors);
        Assert.False(options.EnableCachePreWarming);
        Assert.Equal(System.TimeSpan.FromSeconds(5), options.SchemaRegistrationTimeout);
    }

    [Fact]
    public void UseConfiguration_SetsOptions()
    {
        var config = new Microsoft.Extensions.Configuration.ConfigurationBuilder().AddInMemoryCollection().Build();

        var options = KsqlContextBuilder.Create()
            .UseConfiguration(config)
            .UseSchemaRegistry("http://localhost:8088")
            .Build();

        Assert.Equal(config, options.Configuration);
    }
}
