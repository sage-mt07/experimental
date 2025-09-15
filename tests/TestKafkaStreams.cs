using System;
using System.Collections.Generic;
using System.Threading.Tasks;

#nullable enable

namespace Kafka.Ksql.Linq.Tests;

internal class TestKeyValueStore<TKey, TValue> where TKey : notnull
{
    private readonly Dictionary<TKey, TValue> _dict = new();
    public void Add(TKey key, TValue value) => _dict[key] = value;
    public TValue? Get(TKey key)
    {
        _dict.TryGetValue(key, out var v);
        return v;
    }
}

internal class TestKafkaStreams
{
    private readonly object _store;
    public TestKafkaStreams(object store) => _store = store;
    public Task WaitUntilRunningAsync(string storeName, TimeSpan? timeout = null) => Task.CompletedTask;
    public TStore Store<TStore>(string storeName) => (TStore)_store;
}

