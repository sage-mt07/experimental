using Kafka.Ksql.Linq.Cache.Core;
using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Configuration;
using Kafka.Ksql.Linq.Messaging.Consumers;
using Kafka.Ksql.Linq.Messaging.Producers;
using Kafka.Ksql.Linq.Query.Abstractions;
using System;
using System.Net.Http;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Application;

public class KsqlContextTests
{
    private class TestContext : KsqlContext
    {
        public TestContext() : base(new KsqlDslOptions()) { }
        public TestContext(KsqlDslOptions opt) : base(opt) { }

        protected override bool SkipSchemaRegistration => true;

        public IEntitySet<T> CallCreateEntitySet<T>(EntityModel model) where T : class
            => base.CreateEntitySet<T>(model);

        public KafkaProducerManager CallGetProducerManager() => base.GetProducerManager();
        public KafkaConsumerManager CallGetConsumerManager() => base.GetConsumerManager();
        public IDlqProducer CallGetDlqProducer() => base.GetDlqProducer();

        public Uri GetBaseAddress()
        {
            var field = typeof(KsqlContext).GetField("_ksqlDbClient", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
            var lazy = (Lazy<HttpClient>)field.GetValue(this)!;
            return lazy.Value.BaseAddress!;
        }
    }

    [Fact]
    public void Constructors_InitializeManagers()
    {
        var ctx = new TestContext();
        Assert.NotNull(ctx.CallGetProducerManager());
        Assert.NotNull(ctx.CallGetConsumerManager());
        Assert.Contains("schema auto-registration ready", ctx.ToString());
    }

    [Fact]
    public void CreateEntitySet_ReturnsEventSet()
    {
        var ctx = new TestContext();
        var model = new EntityModel
        {
            EntityType = typeof(TestEntity),
            TopicName = "test-topic",
            KeyProperties = new[] { typeof(TestEntity).GetProperty(nameof(TestEntity.Id))! },
            AllProperties = typeof(TestEntity).GetProperties()
        };
        model.SetStreamTableType(StreamTableType.Table);
        var set = ctx.CallCreateEntitySet<TestEntity>(model);
        Assert.IsType<ReadCachedEntitySet<TestEntity>>(set);
    }

    [Fact]
    public void Dispose_DoesNotThrow()
    {
        var ctx = new TestContext();
        ctx.Dispose();
    }

    [Fact]
    public void GetDlqProducer_ReturnsInstance()
    {
        var ctx = new TestContext();
        Assert.NotNull(ctx.CallGetDlqProducer());
    }

    [Fact(Skip = "Requires HTTP client wiring")]
    public void KsqlDbUrl_OverridesSchemaRegistryPort()
    {
        var opt = new KsqlDslOptions
        {
            SchemaRegistry = new SchemaRegistrySection { Url = "http://localhost:8081" },
            KsqlDbUrl = "http://example:9000"
        };
        var ctx = new TestContext(opt);
        Assert.Equal("http://example:9000/", ctx.GetBaseAddress().ToString());
    }
}
