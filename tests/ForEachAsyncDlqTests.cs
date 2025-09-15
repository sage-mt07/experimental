using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Modeling;
using Kafka.Ksql.Linq.Messaging;
using Kafka.Ksql.Linq.Messaging.Consumers;
using Kafka.Ksql.Linq.Messaging.Producers;
using Moq;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

#nullable enable

namespace Kafka.Ksql.Linq.Tests;

public class ForEachAsyncDlqTests
{
    private class TestContext : KsqlContext
    {
        public TestContext() : base(new KsqlDslOptions()) { }
        protected override bool SkipSchemaRegistration => true;
    }

    private class DlqSet : EventSet<TestEntity>
    {
        private readonly (TestEntity, Dictionary<string, string>, MessageMeta) _item;
        public DlqSet(TestContext ctx, EntityModel model, IDlqProducer dlq, ICommitManager cm)
            : base(ctx, model, null, dlq, cm)
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
    public async Task FinalFailure_SendsDlq()
    {
        var dlq = new Mock<IDlqProducer>();
        var cm = new Mock<ICommitManager>();
        var ctx = new TestContext();
        var model = CreateModel();
        var set = new DlqSet(ctx, model, dlq.Object, cm.Object);

        await set.ForEachAsync((e, h, m) => throw new Exception("fail"));

        dlq.Verify(p => p.ProduceAsync(It.IsAny<DlqEnvelope>(), It.IsAny<CancellationToken>()), Times.Once);
        cm.Verify(c => c.Commit(It.IsAny<TestEntity>()), Times.Never);
    }
}
