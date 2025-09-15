using Kafka.Ksql.Linq.Mapping;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
namespace Kafka.Ksql.Linq.Cache.Core;

internal class TableCache<T> : ITableCache<T> where T : class
{
    private const char KeySep = '\u0000'; // NUL
    private readonly MappingRegistry? _mappingRegistry;
    private readonly string _storeName;
    private readonly Func<TimeSpan?, Task> _waitUntilRunning;
    private readonly Lazy<Func<IEnumerable<(object key, object val)>>> _enumerateLazy;
    private readonly Func<object, string>? _testKeyStringifier;
    private readonly Func<string, object, Type, object>? _testCombiner;

    public TableCache(MappingRegistry mappingRegistry, string storeName,
                      Func<TimeSpan?, Task> waitUntilRunning,
                      Lazy<Func<IEnumerable<(object key, object val)>>> enumerateLazy)
    {
        _mappingRegistry = mappingRegistry;
        _storeName = storeName;
        _waitUntilRunning = waitUntilRunning;
        _enumerateLazy = enumerateLazy;
    }

    internal TableCache(
        Func<TimeSpan?, Task> waitUntilRunning,
        Lazy<Func<IEnumerable<(object key, object val)>>> enumerateLazy,
        Func<object, string> keyStringifier,
        Func<string, object, Type, object> combiner)
    {
        _mappingRegistry = null!;            // not used in this constructor
        _storeName = "test";
        _waitUntilRunning = waitUntilRunning;
        _enumerateLazy = enumerateLazy;
        _testKeyStringifier = keyStringifier;
        _testCombiner = combiner;
    }
    public async Task<List<T>> ToListAsync(List<string>? filter = null, TimeSpan? timeout = null)
    {
        await _waitUntilRunning(timeout);
        var mapping = _mappingRegistry is null ? null : _mappingRegistry.GetMapping(typeof(T));

        string? prefix = null;
        if (filter is { Count: > 0 })
            prefix = string.Join(KeySep, filter) + KeySep;

        var list = new List<T>();
        foreach (var (key, val) in _enumerateLazy.Value())
        {
            // Key is expected to be a string (stringified by the Streamiz side)
            var keyStr = key as string
                          ?? _testKeyStringifier?.Invoke(key)
                          ?? key?.ToString();
            if (keyStr == null)
                continue;

            if (prefix != null && !keyStr.StartsWith(prefix, StringComparison.Ordinal))
                continue;

            if (_testCombiner is not null)
                list.Add((T)_testCombiner(keyStr, val, typeof(T)));
            else
                list.Add((T)mapping!.CombineFromStringKeyAndAvroValue(keyStr, val, typeof(T)));
        }
        return list;
    }

    public void Dispose() { }
}
