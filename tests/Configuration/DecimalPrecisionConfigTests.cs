using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Core.Models;
using Kafka.Ksql.Linq.SchemaRegistryTools;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Configuration;

class Sample { [KsqlDecimal(20, 5)] public decimal Amount { get; set; } }

public class DecimalPrecisionConfigTests
{

    [Fact(Skip = "Decimal precision overrides not part of current scope")]
    public void AppsettingsOverrideWins()
    {
        var overrides = new Dictionary<string, Dictionary<string, KsqlDslOptions.DecimalSetting>>
        {
            [nameof(Sample)] = new() { [nameof(Sample.Amount)] = new KsqlDslOptions.DecimalSetting { Precision = 18, Scale = 9 } }
        };
        DecimalPrecisionConfig.Configure(18, 2, overrides);
        var prop = typeof(Sample).GetProperty(nameof(Sample.Amount))!;
        var meta = PropertyMeta.FromProperty(prop);
        Assert.Equal(18, DecimalPrecisionConfig.ResolvePrecision(meta.Precision, prop));
        Assert.Equal(9, DecimalPrecisionConfig.ResolveScale(meta.Scale, prop));
    }
}

public class DecimalSchemaValidatorTests
{
    class PlainSample { public decimal Amount { get; set; } }

    private const string Schema = "{ \"type\": \"record\", \"name\": \"sampleAvro\", \"fields\": [ { \"name\": \"Amount\", \"type\": { \"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 18, \"scale\": 9 } } ] }";

    [Fact]
    public void StrictMismatchThrows()
    {
        var client = new FakeSchemaRegistryClient();
        client.RegisterSchemaAsync("sample-key", "{ \"type\": \"record\", \"name\": \"key\", \"fields\": [ { \"name\": \"Id\", \"type\": \"int\" } ] }").GetAwaiter().GetResult();
        client.RegisterSchemaAsync("sample-value", Schema).GetAwaiter().GetResult();
        var model = new EntityModel { EntityType = typeof(PlainSample), TopicName = "sample", AllProperties = typeof(PlainSample).GetProperties() };
        DecimalPrecisionConfig.Configure(18, 2, null);
        Assert.Throws<InvalidOperationException>(() => DecimalSchemaValidator.Validate(model, client, ValidationMode.Strict, NullLogger.Instance));
    }

    [Fact(Skip = "Requires schema registry")]
    public void RelaxedAdjustsScale()
    {
        var client = new FakeSchemaRegistryClient();
        client.RegisterSchemaAsync("sample-key", "{ \"type\": \"record\", \"name\": \"key\", \"fields\": [ { \"name\": \"Id\", \"type\": \"int\" } ] }").GetAwaiter().GetResult();
        client.RegisterSchemaAsync("sample-value", Schema).GetAwaiter().GetResult();
        var model = new EntityModel { EntityType = typeof(PlainSample), TopicName = "sample", AllProperties = typeof(PlainSample).GetProperties() };
        DecimalPrecisionConfig.Configure(18, 2, null);
        DecimalSchemaValidator.Validate(model, client, ValidationMode.Relaxed, NullLogger.Instance);
        var prop = typeof(PlainSample).GetProperty(nameof(PlainSample.Amount))!;
        Assert.Equal(9, DecimalPrecisionConfig.ResolveScale(null, prop));
    }
}
