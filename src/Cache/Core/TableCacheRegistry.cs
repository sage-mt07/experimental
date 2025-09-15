using Kafka.Ksql.Linq.Core.Abstractions;
using System;
using System.Collections.Generic;

namespace Kafka.Ksql.Linq.Cache.Core;

internal class TableCacheRegistry : IDisposable
{
    private readonly Dictionary<Type, object> _caches = new();

    public void Register(Type type, object cache)
    {
        _caches[type] = cache;
    }

    public void RegisterEligibleTables(IEnumerable<EntityModel> models, HashSet<string> tableTopics)
    {
        // no-op for simplified registry
    }

    public ITableCache<T>? GetCache<T>() where T : class
    {
        return _caches.TryGetValue(typeof(T), out var c) ? (ITableCache<T>)c : null;
    }

    public void Dispose()
    {
        foreach (var c in _caches.Values)
            (c as IDisposable)?.Dispose();
        _caches.Clear();
    }
}
