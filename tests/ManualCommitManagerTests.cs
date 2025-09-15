using Confluent.Kafka;
using Kafka.Ksql.Linq;
using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Modeling;
using Kafka.Ksql.Linq.Messaging;
using Kafka.Ksql.Linq.Messaging.Consumers;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Kafka.Ksql.Linq.Tests;

public class ManualCommitManagerTests
{
    public sealed class FakeConsumer
    {
        public readonly List<TopicPartitionOffset> Calls = new();
        public void Commit(TopicPartitionOffset tpo) => Calls.Add(tpo);
        public void Commit(IEnumerable<TopicPartitionOffset> tpos)
        {
            foreach (var t in tpos) Calls.Add(t);
        }
    }

    private static MessageMeta Meta(string topic, int partition, long offset)
        => new MessageMeta(topic, partition, offset, DateTimeOffset.UtcNow, null, null, false, new Dictionary<string, string>());

    private sealed class TestCtx : KsqlContext
    {
        public TestCtx() : base(new KsqlDslOptions()) { }
        protected override bool SkipSchemaRegistration => true;
    }

    private static EntityModel CreateModelLocal()
    {
        var b = new ModelBuilder();
        b.Entity<TestEntity>();
        return b.GetEntityModel<TestEntity>()!;
    }

    [Fact]
    public void TrackThenCommit_CommitsOffsetPlusOne()
    {
        var cm = new ManualCommitManager();
        var consumer = new FakeConsumer();
        cm.Bind(typeof(TestEntity), "manual_commit", consumer);

        var ent = new TestEntity { Id = 123 };
        var registrar = (EventSet<object>.ICommitRegistrar)cm; // InternalsVisibleTo
        registrar.Track(ent, Meta("manual_commit", 0, 2));

        cm.Commit(ent);

        Assert.Single(consumer.Calls);
        var tpo = consumer.Calls[0];
        Assert.Equal("manual_commit", tpo.Topic);
        Assert.Equal(0, tpo.Partition.Value);
        Assert.Equal(3, tpo.Offset.Value); // offset + 1
    }

    [Fact]
    public void DuplicateCommit_IsIgnored()
    {
        var cm = new ManualCommitManager();
        var consumer = new FakeConsumer();
        cm.Bind(typeof(TestEntity), "t", consumer);

        var ent = new TestEntity { Id = 1 };
        var registrar = (EventSet<object>.ICommitRegistrar)cm;
        registrar.Track(ent, Meta("t", 0, 5));

        cm.Commit(ent);
        cm.Commit(ent); // second call should be ignored

        Assert.Single(consumer.Calls);
        Assert.Equal(6, consumer.Calls[0].Offset.Value);
    }

    [Fact]
    public void OutOfOrderCommits_AreHandledSafely()
    {
        var cm = new ManualCommitManager();
        var consumer = new FakeConsumer();
        cm.Bind(typeof(TestEntity), "t2", consumer);

        var registrar = (EventSet<object>.ICommitRegistrar)cm;
        var e1 = new TestEntity { Id = 1 };
        var e2 = new TestEntity { Id = 2 };
        var e3 = new TestEntity { Id = 3 };

        registrar.Track(e1, Meta("t2", 0, 1));
        registrar.Track(e2, Meta("t2", 0, 2));
        registrar.Track(e3, Meta("t2", 0, 3));

        cm.Commit(e2); // commit at 3
        cm.Commit(e3); // commit at 4
        cm.Commit(e1); // older -> ignored

        Assert.Equal(2, consumer.Calls.Count);
        Assert.Equal(3, consumer.Calls[0].Offset.Value);
        Assert.Equal(4, consumer.Calls[1].Offset.Value);
    }

    [Fact]
    public async Task ForEachAsync_OnError_ManualCommitIsCalled_WhenAutoCommitFalse()
    {
        var cm = new Mock<ICommitManager>();
        var ctx = new TestCtx();
        var model = CreateModelLocal();
        var set = new ThrowingSingleItemSet(ctx, model, cm.Object);

        await set.ForEachAsync((e, h, m) => throw new InvalidOperationException("boom"), autoCommit: false);
        cm.Verify(x => x.Commit(It.IsAny<object>()), Times.Once);
    }

    [Fact]
    public async Task ForEachAsync_OnError_DoesNotCommit_WhenAutoCommitTrue()
    {
        var cm = new Mock<ICommitManager>();
        var ctx = new TestCtx();
        var model = CreateModelLocal();
        var set = new ThrowingSingleItemSet(ctx, model, cm.Object);

        await set.ForEachAsync((e, h, m) => throw new InvalidOperationException("boom"), autoCommit: true);
        cm.Verify(x => x.Commit(It.IsAny<object>()), Times.Never);
    }

    private sealed class ThrowingSingleItemSet : EventSet<TestEntity>
    {
        private readonly (TestEntity, Dictionary<string, string>, MessageMeta) _item;
        public ThrowingSingleItemSet(KsqlContext ctx, EntityModel model, ICommitManager cm)
            : base(ctx, model, commitManager: cm)
        {
            _item = (new TestEntity { Id = 1 }, new(), Meta("x", 0, 0));
        }

        protected override Task SendEntityAsync(TestEntity entity, Dictionary<string, string>? headers, System.Threading.CancellationToken cancellationToken) => Task.CompletedTask;

        protected override async IAsyncEnumerable<(TestEntity Entity, Dictionary<string, string> Headers, MessageMeta Meta)> ConsumeAsync(KsqlContext context, bool autoCommit, [System.Runtime.CompilerServices.EnumeratorCancellation] System.Threading.CancellationToken cancellationToken)
        {
            yield return _item;
            await Task.CompletedTask;
        }

        public override async IAsyncEnumerator<TestEntity> GetAsyncEnumerator(System.Threading.CancellationToken cancellationToken = default)
        {
            await Task.CompletedTask;
            yield break;
        }
    }

    [Fact]
    public void RegistrarPresence_DetectedCorrectly()
    {
        var cm = new ManualCommitManager();
        Assert.IsAssignableFrom<EventSet<object>.ICommitRegistrar>(cm);

        var mock = new Mock<ICommitManager>();
        Assert.False(mock.Object is EventSet<object>.ICommitRegistrar);
    }
}
