using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Query.Abstractions;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

#nullable enable

namespace Kafka.Ksql.Linq.Tests;

public class StreamTableInterfaceRestrictionTests
{
    private class DummyContext : IKsqlContext
    {
        public IEntitySet<T> Set<T>() where T : class => throw new NotImplementedException();
        public object GetEventSet(Type entityType) => throw new NotImplementedException();
        public Dictionary<Type, EntityModel> GetEntityModels() => new();
        public void Dispose() { }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private class SimpleSet : EventSet<TestEntity>
    {
        public SimpleSet(EntityModel model) : base(new DummyContext(), model) { }

        protected override Task SendEntityAsync(TestEntity entity, Dictionary<string, string>? headers, CancellationToken cancellationToken) => Task.CompletedTask;

        public override Task ForEachAsync(Func<TestEntity, Task> action, TimeSpan timeout = default, bool autoCommit = true, CancellationToken cancellationToken = default) => Task.CompletedTask;

        public override async IAsyncEnumerator<TestEntity> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            await Task.CompletedTask;
            yield break;
        }
    }

    private static EntityModel CreateTableModel()
    {
        var model = new EntityModel
        {
            EntityType = typeof(TestEntity),
            TopicName = "t",
            AllProperties = typeof(TestEntity).GetProperties(),
            KeyProperties = new[] { typeof(TestEntity).GetProperty(nameof(TestEntity.Id))! }
        };
        model.SetStreamTableType(StreamTableType.Table);
        return model;
    }

    private static EntityModel CreateStreamModel()
    {
        var model = new EntityModel
        {
            EntityType = typeof(TestEntity),
            TopicName = "s",
            AllProperties = typeof(TestEntity).GetProperties(),
            KeyProperties = Array.Empty<PropertyInfo>()
        };
        model.SetStreamTableType(StreamTableType.Stream);
        return model;
    }

    [Fact]
    public async Task Table_ForEachAsync_Allows()
    {
        var set = new SimpleSet(CreateTableModel());
        await set.ForEachAsync(_ => Task.CompletedTask);
    }

    [Fact]
    public async Task Stream_ToListAsync_Throws()
    {
        var set = new SimpleSet(CreateStreamModel());
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => set.ToListAsync());
        Assert.Contains("ToListAsync() is not supported", ex.Message);
    }

    [Fact]
    public async Task Table_ToListAsync_Allows()
    {
        var set = new SimpleSet(CreateTableModel());
        var list = await set.ToListAsync();
        Assert.NotNull(list);
    }

    [Fact]
    public async Task Stream_ForEachAsync_Allows()
    {
        var set = new SimpleSet(CreateStreamModel());
        await set.ForEachAsync(_ => Task.CompletedTask);
    }
}
