using Confluent.SchemaRegistry;
using Kafka.Ksql.Linq.Application;
using System;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Application;

public class KsqlContextOptionsTests
{
    [Fact]
    public void Validate_WithMissingClient_Throws()
    {
        var options = new KsqlContextOptions { SchemaRegistryClient = null! };
        var ex = Assert.Throws<InvalidOperationException>(() => options.Validate());
        Assert.Contains("SchemaRegistryClient", ex.Message);
    }

    [Fact]
    public void Validate_WithNonPositiveTimeout_Throws()
    {
        var options = new KsqlContextOptions { SchemaRegistryClient = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = "url" }), SchemaRegistrationTimeout = System.TimeSpan.Zero };
        Assert.Throws<InvalidOperationException>(() => options.Validate());
    }

    [Fact]
    public void Validate_WithValidOptions_DoesNotThrow()
    {
        var options = new KsqlContextOptions { SchemaRegistryClient = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = "url" }) };
        options.Validate();
    }
}
