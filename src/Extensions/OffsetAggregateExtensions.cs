using System;
using System.Linq;
using System.Linq.Expressions;

namespace Kafka.Ksql.Linq
{
    /// <summary>
    /// Extension methods for KSQL offset aggregate functions.
    /// These are not used at runtime and exist only for LINQ expression analysis.
    /// </summary>
    public static class OffsetAggregateExtensions
    {
        public static TResult LatestByOffset<TSource, TKey, TResult>(this IGrouping<TKey, TSource> source, Expression<Func<TSource, TResult>> selector)
        {
            throw new NotSupportedException("LatestByOffset is for expression translation only.");
        }

        public static TResult EarliestByOffset<TSource, TKey, TResult>(this IGrouping<TKey, TSource> source, Expression<Func<TSource, TResult>> selector)
        {
            throw new NotSupportedException("EarliestByOffset is for expression translation only.");
        }
    }
}
