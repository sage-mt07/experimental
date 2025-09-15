using Kafka.Ksql.Linq.Core.Models;
using Kafka.Ksql.Linq.Mapping;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Kafka.Ksql.Linq.Runtime.Heartbeat;

public interface IMarketScheduleProvider
{
    Task InitializeAsync(Type scheduleType, IEnumerable rows, CancellationToken ct);
    Task RefreshAsync(Type scheduleType, IEnumerable rows, CancellationToken ct);
    bool IsInSession(IReadOnlyList<string> keyParts, DateTime utcTs);
}

internal sealed class MarketScheduleProvider : IMarketScheduleProvider
{
    private readonly MappingRegistry _registry;
    private readonly Dictionary<string, List<(DateTime OpenUtc, DateTime CloseUtc)>> _index = new();
    private PropertyMeta[] _keyMeta = System.Array.Empty<PropertyMeta>();

    public MarketScheduleProvider(MappingRegistry registry) => _registry = registry;

    public Task InitializeAsync(Type scheduleType, IEnumerable rows, CancellationToken ct)
    {
        BuildIndex(scheduleType, rows);
        return Task.CompletedTask;
    }

    public Task RefreshAsync(Type scheduleType, IEnumerable rows, CancellationToken ct)
    {
        BuildIndex(scheduleType, rows);
        return Task.CompletedTask;
    }

    public bool IsInSession(IReadOnlyList<string> keyParts, DateTime utcTs)
    {
        var key = string.Join("\0", keyParts);
        if (!_index.TryGetValue(key, out var list))
            return false;
        var lo = 0;
        var hi = list.Count - 1;
        while (lo <= hi)
        {
            var mid = (lo + hi) / 2;
            var (open, close) = list[mid];
            if (utcTs < open)
                hi = mid - 1;
            else if (utcTs >= close)
                lo = mid + 1;
            else
                return true;
        }
        return false;
    }

    private void BuildIndex(Type scheduleType, IEnumerable rows)
    {
        _index.Clear();
        var mapping = _registry.GetMapping(scheduleType);
        _keyMeta = mapping.KeyProperties;
        var openProp = scheduleType.GetProperty("Open")!;
        var closeProp = scheduleType.GetProperty("Close")!;
        foreach (var r in rows)
        {
            var parts = new string[_keyMeta.Length];
            for (int i = 0; i < _keyMeta.Length; i++)
                parts[i] = Convert.ToString(_keyMeta[i].PropertyInfo!.GetValue(r)) ?? string.Empty;
            var key = string.Join("\0", parts);
            if (!_index.TryGetValue(key, out var list))
            {
                list = new List<(DateTime, DateTime)>();
                _index[key] = list;
            }
            var open = ((DateTime)openProp.GetValue(r)!).ToUniversalTime();
            var close = ((DateTime)closeProp.GetValue(r)!).ToUniversalTime();
            list.Add((open, close));
        }
        foreach (var list in _index.Values)
            list.Sort((a, b) => a.OpenUtc.CompareTo(b.OpenUtc));
    }
}

