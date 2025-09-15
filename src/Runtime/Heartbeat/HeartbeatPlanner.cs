using System;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Ksql.Linq.Runtime.Heartbeat;

internal sealed class HeartbeatPlanner
{
    private readonly TimeSpan _grace;
    private readonly List<HeartbeatItem> _items;
    private readonly IMarketScheduleProvider _provider;

    public HeartbeatPlanner(TimeSpan grace, IEnumerable<HeartbeatItem> items, IMarketScheduleProvider provider)
    {
        _grace = grace;
        _items = items.ToList();
        _provider = provider;
    }

    public IEnumerable<HeartbeatItem> Plan(DateTime nowUtc)
    {
        foreach (var i in _items)
        {
            if (i.BucketStartUtc.AddMinutes(1).Add(_grace) <= nowUtc &&
                _provider.IsInSession(i.KeyParts, i.BucketStartUtc))
                yield return i;
        }
    }
}

internal sealed record HeartbeatItem(IReadOnlyList<string> KeyParts, DateTime BucketStartUtc);

