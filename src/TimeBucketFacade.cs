using System;
using Kafka.Ksql.Linq.Runtime;

namespace Kafka.Ksql.Linq;

/// <summary>
/// Facade for TimeBucket APIs to avoid exposing the Runtime namespace
/// in call sites. Forwards to Kafka.Ksql.Linq.Runtime.TimeBucket.
/// </summary>
public static class TimeBucket
{
    // New overload: allow using KsqlContext directly
    public static Runtime.TimeBucket<T> Get<T>(KsqlContext context, Runtime.Period period) where T : class
    {
        if (period.Unit == Kafka.Ksql.Linq.Runtime.PeriodUnit.Seconds)
            throw new ArgumentOutOfRangeException(nameof(period), "Period must be minutes or greater.");
        return Runtime.TimeBucket.Get<T>(context, period);
    }

    // New overload: writer using KsqlContext directly
    public static Runtime.TimeBucketWriter<T> Set<T>(KsqlContext context, Runtime.Period period) where T : class
    {
        if (period.Unit == Kafka.Ksql.Linq.Runtime.PeriodUnit.Seconds)
            throw new ArgumentOutOfRangeException(nameof(period), "Period must be minutes or greater.");
        return Runtime.TimeBucket.Set<T>(context, period);
    }
}
