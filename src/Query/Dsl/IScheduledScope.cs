using System;
using System.Linq.Expressions;

namespace Kafka.Ksql.Linq.Query.Dsl;

public interface IScheduledScope<T>
{
    KsqlQueryable<T> Tumbling(
        Expression<Func<T, DateTime>> time,
        Windows windows,
        int baseUnitSeconds = 10,
        TimeSpan? grace = null);
}
