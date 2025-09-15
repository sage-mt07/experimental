using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Modeling;
using Kafka.Ksql.Linq.Messaging;
using Kafka.Ksql.Linq.Messaging.Internal;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

#nullable enable

namespace Kafka.Ksql.Linq.Tests;

public class ForEachAsyncRetryTests
{
    private class TestContext : KsqlContext
    {
        public TestContext() : base(new KsqlDslOptions()) { }
        protected override bool SkipSchemaRegistration => true;
    }

    private class RetrySet : EventSet<TestEntity>
    {
        private readonly List<(TestEntity, Dictionary<string, string>)> _items;
        public RetrySet(TestContext ctx, EntityModel model, params (TestEntity, Dictionary<string, string>)[] items)
            : base(ctx, model)
        {
            _items = new List<(TestEntity, Dictionary<string, string>)>(items);
        }

        private RetrySet(List<(TestEntity, Dictionary<string, string>)> items, IKsqlContext ctx, EntityModel model, ErrorHandlingContext errorCtx)
            : base(ctx, model)
        {
            _items = items;
            typeof(EventSet<TestEntity>).GetField("_errorHandlingContext", BindingFlags.NonPublic | BindingFlags.Instance)!
                .SetValue(this, errorCtx);
        }
        protected override Task SendEntityAsync(TestEntity entity, Dictionary<string, string>? headers, CancellationToken cancellationToken)
            => Task.CompletedTask;
        protected override async IAsyncEnumerable<(TestEntity Entity, Dictionary<string, string> Headers, MessageMeta Meta)> ConsumeAsync(KsqlContext context, bool autoCommit, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            foreach (var (e, h) in _items)
            {
                var meta = new MessageMeta("t", 0, 0, DateTime.UtcNow, null, null, false, new Dictionary<string, string>());
                yield return (e, h, meta);
                await Task.Yield();
            }
        }

        protected override EventSet<TestEntity> CreateNewInstance(IKsqlContext context, EntityModel model, ErrorHandlingContext errorCtx, IErrorSink? dlqErrorSink)
        {
            return new RetrySet(_items, context, model, errorCtx);
        }

        public override async IAsyncEnumerator<TestEntity> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            foreach (var (entity, _) in _items)
            {
                yield return entity;
                await Task.Yield();
            }
        }
    }

    private static EntityModel CreateModel()
    {
        var builder = new ModelBuilder();
        builder.Entity<TestEntity>();
        return builder.GetEntityModel<TestEntity>()!;
    }

    [Fact]
    public async Task ForEachAsync_Retries_WhenConfigured()
    {
        var ctx = new TestContext();
        var model = CreateModel();
        var item = new TestEntity { Id = 1 };
        var set = new RetrySet(ctx, model, (item, new Dictionary<string, string>()));

        var attempts = 0;
        await set.OnError(ErrorAction.Retry)
            .WithRetry(1, TimeSpan.FromMilliseconds(1))
            .ForEachAsync((_, __, ___) =>
            {
                attempts++;
                if (attempts == 1)
                    throw new System.Exception("fail");
                return Task.CompletedTask;
            });

        Assert.Equal(2, attempts);
    }
}
