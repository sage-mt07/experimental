using System;
using System.Linq;

namespace Kafka.Ksql.Linq
{
    public static class WindowExtensions
    {
        public static DateTime WindowStart<TSource, TKey>(this IGrouping<TKey, TSource> source)
        {
            throw new NotSupportedException("WindowStart is for expression translation only.");
        }
    }
}
