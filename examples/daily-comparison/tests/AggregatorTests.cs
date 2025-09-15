using DailyComparisonLib;
using DailyComparisonLib.Models;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Modeling;
using Kafka.Ksql.Linq.Core.Context;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace DailyComparisonLib.Tests;

public class AggregatorTests
{
    private class DummySet<T> : IQueryable<T>, IEntitySet<T> where T : class
    {
        private readonly List<T> _items;
        private IQueryable<T> _query;
        private readonly IKsqlContext _context;
        private readonly EntityModel _model;
        public DummySet(IKsqlContext context)
        {
            _items = new List<T>();
            _query = _items.AsQueryable();
            _context = context;
            _model = new EntityModel { EntityType = typeof(T), TopicName = typeof(T).Name.ToLowerInvariant(), AllProperties = typeof(T).GetProperties(), KeyProperties = Array.Empty<System.Reflection.PropertyInfo>() };
        }
        public Type ElementType => _query.ElementType;
        public Expression Expression => _query.Expression;
        public IQueryProvider Provider => _query.Provider;
        public Task AddAsync(T entity, CancellationToken cancellationToken = default) { _items.Add(entity); _query = _items.AsQueryable(); return Task.CompletedTask; }
        public void AddItem(T item) { _items.Add(item); _query = _items.AsQueryable(); }
        public Task<List<T>> ToListAsync(CancellationToken cancellationToken = default) => Task.FromResult(_items.ToList());
        public Task ForEachAsync(Func<T, Task> action, TimeSpan timeout = default, CancellationToken cancellationToken = default) => Task.WhenAll(_items.Select(action));
        public string GetTopicName() => _model.TopicName ?? typeof(T).Name.ToLowerInvariant();
        public EntityModel GetEntityModel() => _model;
        public IKsqlContext GetContext() => _context;
        public async IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            foreach (var i in _items)
            {
                yield return i;
                await Task.Yield();
            }
        }
        public IEnumerator<T> GetEnumerator() => _items.GetEnumerator();
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => _items.GetEnumerator();
    }

    private class DummyContext : IKsqlContext
    {
        private readonly Dictionary<Type, object> _sets = new();
        public void AddSet<T>(DummySet<T> set) where T : class => _sets[typeof(T)] = set;
        public IEntitySet<T> Set<T>() where T : class => (IEntitySet<T>)_sets[typeof(T)];
        public object GetEventSet(Type entityType) => _sets[entityType];
        public Dictionary<Type, EntityModel> GetEntityModels() => new();
        public void Dispose() { }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    [Fact]
    public async Task Aggregate_ComputesDailyComparison()
    {
        var context = new DummyContext();
        var rateSet = new DummySet<Rate>(context);
        var scheduleSet = new DummySet<MarketSchedule>(context);
        var dailySet = new DummySet<DailyComparison>(context);
        var candleSet = new DummySet<RateCandle>(context);
        candleSet.GetEntityModel().BarTimeSelector = (Expression<Func<RateCandle, DateTime>>)(x => x.BarTime);
        context.AddSet(rateSet);
        context.AddSet(scheduleSet);
        context.AddSet(dailySet);
        context.AddSet(candleSet);

        var daily = new DailyComparison { Broker = "b", Symbol = "s", Date = new DateTime(2024,1,1), High = 2.1m, Low = 1m, Close = 2.1m, PrevClose = 2m, Diff = 0.1m };
        dailySet.AddItem(daily);

        candleSet.AddItem(new RateCandle { Broker = "b", Symbol = "s", BarTime = new DateTime(2024,1,1,1,0,0), Open = 1.1m, High = 1.1m, Low = 1m, Close = 1.1m });
        candleSet.AddItem(new RateCandle { Broker = "b", Symbol = "s", BarTime = new DateTime(2024,1,1,2,0,0), Open = 2.1m, High = 2.1m, Low = 2m, Close = 2.1m });

        var aggregator = new Aggregator(new KafkaKsqlContextStub(context), null);
        var (dailyBars, minuteBars) = await aggregator.AggregateAsync(new DateTime(2024,1,1));

        var result = Assert.Single(dailyBars);
        Assert.Equal(daily.High, result.High);
        Assert.Equal(daily.Low, result.Low);
        Assert.Equal(daily.Close, result.Close);

        Assert.Equal(2, minuteBars.Count);
    }

    private class KafkaKsqlContextStub : KafkaKsqlContext
    {
        private readonly IKsqlContext _inner;
        public KafkaKsqlContextStub(IKsqlContext inner) : base(new KafkaContextOptions()) { _inner = inner; }
        protected override bool SkipSchemaRegistration => true;
        protected override IEntitySet<T> CreateEntitySet<T>(EntityModel entityModel) where T : class
        {
            return (IEntitySet<T>)_inner.GetEventSet(typeof(T));
        }
    }
}
