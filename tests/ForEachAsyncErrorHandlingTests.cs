using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Modeling;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

#nullable enable

namespace Kafka.Ksql.Linq.Tests;

public class ForEachAsyncErrorHandlingTests
{
    private class DummyContext : IKsqlContext
    {
        public IEntitySet<T> Set<T>() where T : class => throw new NotImplementedException();
        public object GetEventSet(Type entityType) => throw new NotImplementedException();
        public Dictionary<Type, EntityModel> GetEntityModels() => new();
        public void Dispose() { }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private class FaultySet : EventSet<TestEntity>
    {
        private readonly List<TestEntity> _items;
        public FaultySet(List<TestEntity> items, EntityModel model) : base(new DummyContext(), model)
        {
            _items = items;
        }

        protected override Task SendEntityAsync(TestEntity entity, Dictionary<string, string>? headers, CancellationToken cancellationToken) => Task.CompletedTask;

        private class FaultyEnumerator : IAsyncEnumerator<TestEntity>
        {
            private readonly List<TestEntity> _items;
            private int _index = -1;
            private bool _thrown;
            public FaultyEnumerator(List<TestEntity> items) => _items = items;
            public TestEntity Current { get; private set; } = null!;
            public ValueTask DisposeAsync() => ValueTask.CompletedTask;
            public ValueTask<bool> MoveNextAsync()
            {
                _index++;
                if (!_thrown && _index == 1)
                {
                    _thrown = true;
                    return ValueTask.FromException<bool>(new InvalidOperationException("fail"));
                }
                if (_index >= _items.Count)
                    return new ValueTask<bool>(false);
                Current = _items[_index];
                return new ValueTask<bool>(true);
            }
        }

        public override IAsyncEnumerator<TestEntity> GetAsyncEnumerator(CancellationToken cancellationToken = default)
            => new FaultyEnumerator(_items);
    }

    private static EntityModel CreateModel()
    {
        var builder = new ModelBuilder();
        builder.Entity<TestEntity>();
        return builder.GetEntityModel<TestEntity>()!;
    }

    [Fact(Skip = "Requires KsqlContext")]
    public async Task EnumeratorException_IsSkipped()
    {
        var items = new List<TestEntity>
        {
            new TestEntity{ Id = 1 },
            new TestEntity{ Id = 2 },
            new TestEntity{ Id = 3 }
        };
        var set = new FaultySet(items, CreateModel());
        var results = new List<int>();

        await set.ForEachAsync((e, _, _) =>
        {
            results.Add(e.Id);
            return Task.CompletedTask;
        });

        Assert.Equal(new[] { 1, 3 }, results);
    }
}
