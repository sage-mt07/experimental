using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Configuration;
using Kafka.Ksql.Linq.Core.Models;
using Kafka.Ksql.Linq.Mapping;
using Kafka.Ksql.Linq.Messaging.Producers;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Xunit;

#nullable enable

namespace Kafka.Ksql.Linq.Tests.Messaging;

public class KafkaProducerManagerMappingTests
{
    private class KeyedSample
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private class KeylessSample
    {
        public string Name { get; set; } = string.Empty;
    }

    private static MappingRegistry SetupRegistryForKeyed()
    {
        var registry = new MappingRegistry();
        var keyProps = new[] { PropertyMeta.FromProperty(typeof(KeyedSample).GetProperty(nameof(KeyedSample.Id))!) };
        var valueProps = typeof(KeyedSample).GetProperties().Select(p => PropertyMeta.FromProperty(p)).ToArray();
        registry.Register(typeof(KeyedSample), keyProps, valueProps);
        return registry;
    }

    private static MappingRegistry SetupRegistryForKeyless()
    {
        var registry = new MappingRegistry();
        var valueProps = typeof(KeylessSample).GetProperties().Select(p => PropertyMeta.FromProperty(p)).ToArray();
        registry.Register(typeof(KeylessSample), System.Array.Empty<PropertyMeta>(), valueProps);
        return registry;
    }

    [Fact]
    public async Task GetProducerAsync_Keyless_ReturnsKeylessProducer()
    {
        var registry = SetupRegistryForKeyless();
        var options = Options.Create(new KsqlDslOptions
        {
            SchemaRegistry = new SchemaRegistrySection { Url = "http://localhost" }
        });
        var mgr = new KafkaProducerManager(registry, options, new NullLoggerFactory());
        var producerTask = (Task<KafkaProducerManager.ProducerHolder>)Kafka.Ksql.Linq.Tests.PrivateAccessor.InvokePrivate(
            mgr, "GetProducerAsync", new[] { typeof(string) }, new[] { typeof(KeylessSample) }, new object?[] { null! })!;
        var producer = await producerTask;
        Assert.Equal("keylesssample", producer.TopicName);
    }

    [Fact]
    public async Task GetProducerAsync_Keyed_UsesMappingTypes()
    {
        var registry = SetupRegistryForKeyed();
        var options = Options.Create(new KsqlDslOptions
        {
            SchemaRegistry = new SchemaRegistrySection { Url = "http://localhost" }
        });
        var mgr = new KafkaProducerManager(registry, options, new NullLoggerFactory());
        var producerTask = (Task<KafkaProducerManager.ProducerHolder>)Kafka.Ksql.Linq.Tests.PrivateAccessor.InvokePrivate(
            mgr, "GetProducerAsync", new[] { typeof(string) }, new[] { typeof(KeyedSample) }, new object?[] { null! })!;
        var producer = await producerTask;
        Assert.Equal("keyedsample", producer.TopicName);
    }

    private static KafkaProducerManager.ProducerHolder CreateRecordingProducer(out StrongBox<object?> sentKey, out StrongBox<object?> sentValue)
    {
        var keyBox = new StrongBox<object?>(null); var valueBox = new StrongBox<object?>(null);
        sentKey = keyBox; sentValue = valueBox;
        return new KafkaProducerManager.ProducerHolder(
            "t",
            (k, v, c, ct) => { keyBox.Value = k; valueBox.Value = v; return Task.CompletedTask; },
            _ => { },
            () => { });
    }

    [Fact(Skip = "Requires Schema Registry")]
    public async Task SendAsync_PopulatesKeyAndValueObjects()
    {
        var registry = SetupRegistryForKeyed();
        var options = Options.Create(new KsqlDslOptions
        {
            SchemaRegistry = new SchemaRegistrySection { Url = "http://localhost" }
        });
        var mgr = new KafkaProducerManager(registry, options, new NullLoggerFactory());
        var stub = CreateRecordingProducer(out var sentKey, out var sentValue);
        var dict = (ConcurrentDictionary<Type, KafkaProducerManager.ProducerHolder>)typeof(KafkaProducerManager)
            .GetField("_producers", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
            .GetValue(mgr)!;
        dict[typeof(KeyedSample)] = stub;
        var entity = new KeyedSample { Id = 3, Name = "x" };
        await mgr.SendAsync(nameof(KeyedSample).ToLowerInvariant(), entity);
        var mapping = registry.GetMapping(typeof(KeyedSample));
        Assert.IsType(mapping.KeyType, sentKey.Value);
        Assert.IsType(mapping.ValueType, sentValue.Value);
        var combined = (KeyedSample)mapping.CombineFromKeyValue(sentKey.Value, sentValue.Value!, typeof(KeyedSample));
        Assert.Equal(entity.Id, combined.Id);
        Assert.Equal(entity.Name, combined.Name);
    }
}
