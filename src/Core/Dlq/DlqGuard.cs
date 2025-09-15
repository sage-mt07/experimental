using Kafka.Ksql.Linq.Configuration;
using System;

namespace Kafka.Ksql.Linq.Core.Dlq;

public static class DlqGuard
{
    public static bool ShouldSend(DlqOptions o, IRateLimiter limiter, Type exType)
    {
        if (!o.Enabled) return false;
        if (o.IncludedExceptionTypes.Length > 0 &&
            Array.IndexOf(o.IncludedExceptionTypes, exType.Name) < 0) return false;
        if (Array.IndexOf(o.ExcludedExceptionTypes, exType.Name) >= 0) return false;
        if (o.SamplingRate < 1.0 && Random.Shared.NextDouble() > o.SamplingRate) return false;
        if (o.MaxPerSecond > 0 && !limiter.TryAcquire(1)) return false;
        return true;
    }
}

