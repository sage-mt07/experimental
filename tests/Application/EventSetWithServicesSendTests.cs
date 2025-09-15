using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Messaging.Producers;
using System.Runtime.CompilerServices;
using System;

#nullable enable
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Application;

public class EventSetWithServicesSendTests
{
    private static KafkaProducerManager.ProducerHolder CreateStubProducer<T>(out StrongBox<bool> sent) where T : class
    {
        var box = new StrongBox<bool>(false);
        sent = box;
        return new KafkaProducerManager.ProducerHolder(
            "t",
            (k, v, c, ct) => { box.Value = true; return Task.CompletedTask; },
            _ => { },
            () => { });
    }

    private class TestContext : KsqlContext
    {
        public TestContext() : base(new KsqlDslOptions()) { }

        protected override bool SkipSchemaRegistration => true;

        public void SetProducer(object manager)
        {
            typeof(KsqlContext).GetField("_producerManager", BindingFlags.NonPublic | BindingFlags.Instance)!.SetValue(this, manager);
        }
    }

    private class Sample { public int Id { get; set; } }

    private static EntityModel CreateModel() => new()
    {
        EntityType = typeof(Sample),
        TopicName = "t",
        AllProperties = typeof(Sample).GetProperties(),
        KeyProperties = new[] { typeof(Sample).GetProperty(nameof(Sample.Id))! }
    };

    [Fact(Skip = "Requires full Kafka context setup")]
    public async Task SendEntityAsync_UsesProducerManager()
    {
        var ctx = new TestContext();
        var manager = new KafkaProducerManager(
            new Kafka.Ksql.Linq.Mapping.MappingRegistry(),
            Microsoft.Extensions.Options.Options.Create(new KsqlDslOptions()),
            null);
        ctx.SetProducer(manager);

        var stub = CreateStubProducer<Sample>(out var sent);
        var dict = (System.Collections.Concurrent.ConcurrentDictionary<Type, KafkaProducerManager.ProducerHolder>)
            typeof(KafkaProducerManager).GetField("_producers", BindingFlags.NonPublic | BindingFlags.Instance)!
                .GetValue(manager)!;
        dict[typeof(Sample)] = stub;

        var set = new EventSetWithServices<Sample>(ctx, CreateModel());
        await set.AddAsync(new Sample(), null, CancellationToken.None);
        Assert.True(sent.Value);
    }
}
