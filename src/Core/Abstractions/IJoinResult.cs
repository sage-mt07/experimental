using System;
using System.Linq.Expressions;

namespace Kafka.Ksql.Linq.Core.Abstractions;

public interface IJoinResult<TOuter, TInner>
       where TOuter : class
       where TInner : class
{
    IEntitySet<TResult> Select<TResult>(
        Expression<Func<TOuter, TInner, TResult>> resultSelector) where TResult : class;
}
