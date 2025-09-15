using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Messaging;
using Kafka.Ksql.Linq.Query.Abstractions;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

#nullable enable

namespace Kafka.Ksql.Linq.Tests;

public class DlqStreamRestrictionsTests
{
    private class DummyContext : IKsqlContext
    {
        public IEntitySet<T> Set<T>() where T : class => throw new NotImplementedException();
        public object GetEventSet(Type entityType) => throw new NotImplementedException();
        public Dictionary<Type, EntityModel> GetEntityModels() => new();
        public void Dispose() { }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private class DlqSet : EventSet<DlqEnvelope>
    {
        public DlqSet(EntityModel model) : base(new DummyContext(), model) { }

        protected override Task SendEntityAsync(DlqEnvelope entity, Dictionary<string, string>? headers, CancellationToken cancellationToken) => Task.CompletedTask;

        public override IAsyncEnumerator<DlqEnvelope> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            return AsyncEnumerator();

            async IAsyncEnumerator<DlqEnvelope> AsyncEnumerator()
            {
                await Task.CompletedTask;
                yield break;
            }
        }
    }

    private static EntityModel CreateModel()
    {
        var model = new EntityModel
        {
            EntityType = typeof(DlqEnvelope),
            TopicName = "dlq",
            AllProperties = typeof(DlqEnvelope).GetProperties(),
            KeyProperties = Array.Empty<PropertyInfo>()
        };
        model.SetStreamTableType(StreamTableType.Stream);
        return model;
    }

    [Fact(Skip = "Requires KsqlContext")]
    public async Task DlqStream_ForEachAsync_Allows()
    {
        var set = new DlqSet(CreateModel());
        await set.ForEachAsync(_ => Task.CompletedTask);
    }

    [Fact]
    public async Task DlqStream_ToListAsync_Throws()
    {
        var set = new DlqSet(CreateModel());
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => set.ToListAsync());
        Assert.Contains("DLQ", ex.Message);
    }

}
