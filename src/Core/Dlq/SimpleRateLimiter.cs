using System;

namespace Kafka.Ksql.Linq.Core.Dlq;

public sealed class SimpleRateLimiter : IRateLimiter
{
    private readonly int _maxPerSecond;
    private int _count;
    private long _windowStart;

    public SimpleRateLimiter(int maxPerSecond)
    {
        _maxPerSecond = maxPerSecond;
        _windowStart = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
    }

    public bool TryAcquire(int permits)
    {
        if (_maxPerSecond <= 0) return true;
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        if (now != _windowStart)
        {
            _windowStart = now;
            _count = 0;
        }
        if (_count + permits > _maxPerSecond) return false;
        _count += permits;
        return true;
    }
}

