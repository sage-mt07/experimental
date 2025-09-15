using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Modeling;
using Kafka.Ksql.Linq.Messaging.Internal;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

#nullable enable

namespace Kafka.Ksql.Linq.Tests;

public class EventSetWithRetryTests
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
        private readonly List<TestEntity> _items;

        public TestSet(List<TestEntity> items, EntityModel model, IErrorSink? sink = null)
            : base(new DummyContext(), model, sink)
        {
            _items = items;
        }

        private TestSet(List<TestEntity> items, IKsqlContext ctx, EntityModel model, ErrorHandlingContext errorCtx, IErrorSink? sink)
            : base(ctx, model, sink)
        {
            _items = items;
            typeof(EventSet<TestEntity>).GetField("_errorHandlingContext", BindingFlags.NonPublic | BindingFlags.Instance)!
                .SetValue(this, errorCtx);
        }

        protected override EventSet<TestEntity> CreateNewInstance(IKsqlContext context, EntityModel model, ErrorHandlingContext errorCtx, IErrorSink? dlq)
        {
            return new TestSet(_items, context, model, errorCtx, dlq);
        }

        protected override Task SendEntityAsync(TestEntity entity, Dictionary<string, string>? headers, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public override async IAsyncEnumerator<TestEntity> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            foreach (var item in _items)
            {
                if (cancellationToken.IsCancellationRequested)
                    yield break;
                yield return item;
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

    private static ErrorHandlingContext GetContext(EventSet<TestEntity> set)
    {
        var field = typeof(EventSet<TestEntity>).GetField("_errorHandlingContext", BindingFlags.NonPublic | BindingFlags.Instance)!;
        return (ErrorHandlingContext)field.GetValue(set)!;
    }

    [Fact]
    public void WithRetry_DelaySpecified_RegistersContext()
    {
        var set = new TestSet(new(), CreateModel());
        var newSet = set.WithRetry(3, TimeSpan.FromSeconds(2));
        var ctx = GetContext(newSet);
        Assert.NotSame(set, newSet);
        Assert.Equal(3, ctx.RetryCount);
        Assert.Equal(TimeSpan.FromSeconds(2), ctx.RetryInterval);
        Assert.Equal(ErrorAction.Skip, ctx.ErrorAction);
    }

    [Fact]
    public void WithRetry_DefaultDelay_RegistersContext()
    {
        var set = new TestSet(new(), CreateModel());
        var newSet = set.WithRetry(3);
        var ctx = GetContext(newSet);
        Assert.Equal(3, ctx.RetryCount);
        Assert.Equal(TimeSpan.FromSeconds(1), ctx.RetryInterval);
    }
}
