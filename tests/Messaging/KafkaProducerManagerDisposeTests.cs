using Confluent.SchemaRegistry;
using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Models;
using Kafka.Ksql.Linq.Mapping;
using Kafka.Ksql.Linq.Messaging.Producers;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Concurrent;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Messaging;
#nullable enable

public class KafkaProducerManagerDisposeTests
{
    private class Sample { }

    private static KafkaProducerManager.ProducerHolder CreateStubProducer<T>(out StrongBox<bool> disposed, out StrongBox<bool> sent) where T : class
    {
        var dBox = new StrongBox<bool>(false);
        var sBox = new StrongBox<bool>(false);
        disposed = dBox; sent = sBox;
        return new KafkaProducerManager.ProducerHolder(
            "t",
            (k, v, c, ct) => { sBox.Value = true; return Task.CompletedTask; },
            _ => { },
            () => { dBox.Value = true; });
    }



    private static ConcurrentDictionary<Type, KafkaProducerManager.ProducerHolder> GetProducerDict(KafkaProducerManager manager)
        => (ConcurrentDictionary<Type, KafkaProducerManager.ProducerHolder>)typeof(KafkaProducerManager)
            .GetField("_producers", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(manager)!;

    private static ConcurrentDictionary<(Type, string), KafkaProducerManager.ProducerHolder> GetTopicProducerDict(KafkaProducerManager manager)
        => (ConcurrentDictionary<(Type, string), KafkaProducerManager.ProducerHolder>)typeof(KafkaProducerManager)
            .GetField("_topicProducers", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(manager)!;


    private static Lazy<Confluent.SchemaRegistry.ISchemaRegistryClient> GetSchemaLazy(KafkaProducerManager manager)
        => (Lazy<Confluent.SchemaRegistry.ISchemaRegistryClient>)typeof(KafkaProducerManager)
            .GetField("_schemaRegistryClient", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(manager)!;

    private static bool GetDisposedFlag(KafkaProducerManager manager)
        => (bool)typeof(KafkaProducerManager)
            .GetField("_disposed", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(manager)!;

    [Fact]
    public void Dispose_BeforeProducerCreation_DoesNotThrow()
    {
        var manager = new KafkaProducerManager(new MappingRegistry(), Options.Create(new KsqlDslOptions()), new NullLoggerFactory());
        manager.Dispose();

        Assert.True(GetDisposedFlag(manager));
        Assert.Empty(GetProducerDict(manager));
        Assert.Empty(GetTopicProducerDict(manager));
        Assert.False(GetSchemaLazy(manager).IsValueCreated);
    }

    [Fact]
    public void Dispose_WithCachedResources_DisposesAll()
    {
        var manager = new KafkaProducerManager(new MappingRegistry(), Options.Create(new KsqlDslOptions()), new NullLoggerFactory());
        var producers = GetProducerDict(manager);
        var topics = GetTopicProducerDict(manager);
        var schemaLazyField = typeof(KafkaProducerManager).GetField("_schemaRegistryClient", BindingFlags.NonPublic | BindingFlags.Instance)!;
        schemaLazyField.SetValue(manager, new Lazy<ISchemaRegistryClient>(() => new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = "http://localhost" })));

        var p1 = CreateStubProducer<Sample>(out var disposed1, out _);
        var p2 = CreateStubProducer<Sample>(out var disposed2, out _);
        producers[typeof(Sample)] = p1;
        topics[(typeof(Sample), "t")] = p2;

        manager.Dispose();

        Assert.True(disposed1.Value);
        Assert.True(disposed2.Value);
        Assert.Empty(producers);
        Assert.Empty(topics);
        Assert.True(GetDisposedFlag(manager));
    }

    [Fact]
    public async Task Dispose_AfterUse_DisposesProducers()
    {
        var registry = new MappingRegistry();
        registry.Register(typeof(Sample), Array.Empty<PropertyMeta>(), Array.Empty<PropertyMeta>());
        var manager = new KafkaProducerManager(registry, Options.Create(new KsqlDslOptions()), new NullLoggerFactory());
        var producers = GetProducerDict(manager);
        var topics = GetTopicProducerDict(manager);
        var stub = CreateStubProducer<Sample>(out var disposed, out var sent);
        producers[typeof(Sample)] = stub;
        topics[(typeof(Sample), "sample")] = stub;

        await manager.SendAsync("sample", new Sample());
        Assert.True(sent.Value);

        manager.Dispose();
        Assert.True(disposed.Value);
        Assert.True(GetDisposedFlag(manager));
    }
}
