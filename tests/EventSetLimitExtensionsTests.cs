using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Messaging;
using Kafka.Ksql.Linq.Query.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

#nullable enable

namespace Kafka.Ksql.Linq.Tests;

public class EventSetLimitExtensionsTests
{
    private class RateCandle
    {
        public DateTime BarTime { get; set; }
    }

    private class DummyContext : IKsqlContext
    {
        public IEntitySet<T> Set<T>() where T : class => throw new NotImplementedException();
        public object GetEventSet(Type entityType) => throw new NotImplementedException();
        public Dictionary<Type, EntityModel> GetEntityModels() => new();
        public void Dispose() { }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private class DummySet : IRemovableEntitySet<RateCandle>
    {
        private readonly List<RateCandle> _items;
        public List<RateCandle> Removed { get; } = new();
        private readonly EntityModel _model = new()
        {
            EntityType = typeof(RateCandle),
            TopicName = "ratecandle",
            AllProperties = typeof(RateCandle).GetProperties(),
            KeyProperties = Array.Empty<System.Reflection.PropertyInfo>()
        };
        public DummySet(IEnumerable<RateCandle> items)
        {
            _items = items.ToList();
            _model.BarTimeSelector = (Expression<Func<RateCandle, DateTime>>)(x => x.BarTime);
            _model.SetStreamTableType(StreamTableType.Table);
        }
        public Task AddAsync(RateCandle entity, Dictionary<string, string>? headers = null, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task<List<RateCandle>> ToListAsync(CancellationToken cancellationToken = default) => Task.FromResult(_items.ToList());
        public Task ForEachAsync(Func<RateCandle, Task> action, TimeSpan timeout = default, bool autoCommit = true, CancellationToken cancellationToken = default) => Task.WhenAll(_items.Select(action));

        [Obsolete("Use ForEachAsync(Func<RateCandle, Dictionary<string,string>, MessageMeta, Task>)")]
        public Task ForEachAsync(Func<RateCandle, Dictionary<string, string>, Task> action, TimeSpan timeout = default, bool autoCommit = true, CancellationToken cancellationToken = default)
            => Task.WhenAll(_items.Select(i => action(i, new Dictionary<string, string>())));

        public Task ForEachAsync(Func<RateCandle, Dictionary<string, string>, MessageMeta, Task> action, TimeSpan timeout = default, bool autoCommit = true, CancellationToken cancellationToken = default)
            => Task.WhenAll(_items.Select(i => action(i, new Dictionary<string, string>(), new MessageMeta("t", 0, 0, DateTimeOffset.UnixEpoch, null, null, false, new Dictionary<string, string>()))));
        public string GetTopicName() => _model.TopicName!;
        public EntityModel GetEntityModel() => _model;
        public IKsqlContext GetContext() => new DummyContext();
        public async IAsyncEnumerator<RateCandle> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            foreach (var item in _items)
            {
                yield return item;
                await Task.Yield();
            }
        }
        public Task RemoveAsync(RateCandle entity, CancellationToken cancellationToken = default)
        {
            Removed.Add(entity);
            _items.Remove(entity);
            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task Limit_RemovesOldItemsAndReturnsNewest()
    {
        var items = new List<RateCandle>
        {
            new RateCandle { BarTime = new DateTime(2024,1,1,1,0,0) },
            new RateCandle { BarTime = new DateTime(2024,1,1,2,0,0) },
            new RateCandle { BarTime = new DateTime(2024,1,1,3,0,0) }
        };
        var set = new DummySet(items);

        var result = await set.Limit(2);

        Assert.Equal(2, result.Count);
        Assert.Equal(new DateTime(2024, 1, 1, 3, 0, 0), result[0].BarTime);
        Assert.Equal(new DateTime(2024, 1, 1, 2, 0, 0), result[1].BarTime);
        Assert.Single(set.Removed);
        Assert.Equal(new DateTime(2024, 1, 1, 1, 0, 0), set.Removed[0].BarTime);
    }
}
