using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Modeling;
using Kafka.Ksql.Linq.Messaging;
using Kafka.Ksql.Linq.Messaging.Consumers;
using Moq;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

#nullable enable

namespace Kafka.Ksql.Linq.Tests;

public class ManualCommitTests
{
    private class TestContext : KsqlContext
    {
        public TestContext() : base(new KsqlDslOptions()) { }
        protected override bool SkipSchemaRegistration => true;
    }

    private class PropertyContext : KsqlContext
    {
        public PropertyContext() : base(new KsqlDslOptions()) { }
        protected override bool SkipSchemaRegistration => true;
        public EventSet<TestEntity> Entities { get; private set; } = null!;
        protected override void OnModelCreating(IModelBuilder modelBuilder)
            => modelBuilder.Entity<TestEntity>();
    }

    private class ManualCommitSet : EventSet<TestEntity>
    {
        private readonly (TestEntity, Dictionary<string, string>, MessageMeta) _item;
        public ManualCommitSet(TestContext ctx, EntityModel model, ICommitManager cm)
            : base(ctx, model, commitManager: cm)
        {
            _item = (new TestEntity { Id = 1 }, new(), new MessageMeta("t", 0, 1, DateTime.UtcNow, null, null, false, new Dictionary<string, string>()));
        }

        protected override Task SendEntityAsync(TestEntity entity, Dictionary<string, string>? headers, CancellationToken cancellationToken) => Task.CompletedTask;

        protected override async IAsyncEnumerable<(TestEntity Entity, Dictionary<string, string> Headers, MessageMeta Meta)> ConsumeAsync(KsqlContext context, bool autoCommit, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            yield return _item;
            await Task.CompletedTask;
        }

        public override async IAsyncEnumerator<TestEntity> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            await Task.CompletedTask;
            yield break;
        }
    }

    private static EntityModel CreateModel()
    {
        var builder = new ModelBuilder();
        builder.Entity<TestEntity>();
        return builder.GetEntityModel<TestEntity>()!;
    }

    [Fact]
    public async Task Commit_CallsCommitManager()
    {
        var cm = new Mock<ICommitManager>();
        var ctx = new TestContext();
        var set = new ManualCommitSet(ctx, CreateModel(), cm.Object);
        await set.ForEachAsync((e, h, m) => { set.Commit(e); return Task.CompletedTask; }, autoCommit: false);
        cm.Verify(c => c.Commit(It.Is<TestEntity>(x => x.Id == 1)), Times.Once);
    }

    [Fact]
    public void PropertyEventSet_HasCommitManager()
    {
        using var ctx = new PropertyContext();
        var field = typeof(EventSet<TestEntity>).GetField("_commitManager", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(field!.GetValue(ctx.Entities));
    }
}
