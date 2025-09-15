using Kafka.Ksql.Linq;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Modeling;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Kafka.Ksql.Linq.Tests;

public class EventSetAutoCommitPropagationTests
{
    private class DummyContext : KsqlContext
    {
        public DummyContext() : base(new Kafka.Ksql.Linq.Configuration.KsqlDslOptions()) { }
        protected override bool SkipSchemaRegistration => true;
    }

    private class ProbeSet : EventSet<TestEntity>
    {
        public bool? CapturedAutoCommit { get; private set; }

        public ProbeSet(IKsqlContext ctx, EntityModel model) : base(ctx, model) { }

        protected override async IAsyncEnumerable<(TestEntity Entity, Dictionary<string, string> Headers, Kafka.Ksql.Linq.Messaging.MessageMeta Meta)> ConsumeAsync(
            KsqlContext context, bool autoCommit, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            CapturedAutoCommit = autoCommit;
            yield break;
        }

        protected override Task SendEntityAsync(TestEntity entity, Dictionary<string, string>? headers, CancellationToken cancellationToken)
            => Task.CompletedTask;

        public override async IAsyncEnumerator<TestEntity> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            yield break;
        }
    }

    private static EntityModel CreateModel()
    {
        var b = new ModelBuilder();
        b.Entity<TestEntity>();
        return b.GetEntityModel<TestEntity>()!;
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ForEachAsync_PassesAutoCommit_ToConsumeAsync(bool flag)
    {
        using var ctx = new DummyContext();
        var set = new ProbeSet(ctx, CreateModel());
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(10));
        try { await set.ForEachAsync(_ => Task.CompletedTask, autoCommit: flag, cancellationToken: cts.Token); } catch { }
        Assert.Equal(flag, set.CapturedAutoCommit);
    }
}
