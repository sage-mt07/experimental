using System;
using System.Linq.Expressions;

namespace Kafka.Ksql.Linq.Core.Abstractions
{
    public interface IJoinableEntitySet<T> : IEntitySet<T> where T : class
    {
        IJoinResult<T, TInner> Join<TInner, TKey>(
            IEntitySet<TInner> inner,
            Expression<Func<T, TKey>> outerKeySelector,
            Expression<Func<TInner, TKey>> innerKeySelector) where TInner : class;
    }
}
