using Kafka.Ksql.Linq.Core.Abstractions;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
#nullable enable

namespace Kafka.Ksql.Linq.Tests;

public class EventSetCreateMessageContextTests
{
    private class DummyContext : IKsqlContext
    {
        public IEntitySet<T> Set<T>() where T : class => throw new NotImplementedException();
        public object GetEventSet(Type entityType) => throw new NotImplementedException();
        public Dictionary<Type, EntityModel> GetEntityModels() => new();
        public void Dispose() { }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private class TestSet : EventSet<TestEntity>
    {
        public TestSet(EntityModel model) : base(new DummyContext(), model)
        {
        }

        protected override Task SendEntityAsync(TestEntity entity, Dictionary<string, string>? headers, CancellationToken cancellationToken) => Task.CompletedTask;

        public override async IAsyncEnumerator<TestEntity> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            yield break;
        }
    }

    private static EntityModel CreateModel() => new()
    {
        EntityType = typeof(TestEntity),
        TopicName = "test-topic",
        KeyProperties = new[] { typeof(TestEntity).GetProperty(nameof(TestEntity.Id))! },
        AllProperties = typeof(TestEntity).GetProperties()
    };

    [Fact]
    public void CreateMessageContext_ReturnsExpectedContext()
    {
        var set = new TestSet(CreateModel());
        var entity = new TestEntity { Id = 1 };

        var ctx = PrivateAccessor.InvokePrivate<KafkaMessageContext>(
            set,
            "CreateMessageContext",
            new[] { typeof(TestEntity) },
            null,
            entity);

        Assert.NotNull(ctx);
        Assert.False(string.IsNullOrEmpty(ctx.MessageId));
        Assert.Equal("TestEntity", ctx.Tags["entity_type"]);
        Assert.Equal("test-topic", ctx.Tags["topic_name"]);
        Assert.Equal("ForEachAsync", ctx.Tags["processing_phase"]);
        Assert.True(ctx.Tags.ContainsKey("timestamp"));
    }
}
