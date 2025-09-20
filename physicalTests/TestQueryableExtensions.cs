using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Kafka.Ksql.Linq.Tests.Integration;

internal static class TestQueryableExtensions
{
    private static readonly MethodInfo HavingMethodInfo = typeof(TestQueryableExtensions)
        .GetMethod(nameof(Having), BindingFlags.Static | BindingFlags.Public)!
        .GetGenericMethodDefinition();

    public static IQueryable<IGrouping<TKey, TSource>> Having<TKey, TSource>(
        this IQueryable<IGrouping<TKey, TSource>> source,
        Expression<Func<IGrouping<TKey, TSource>, bool>> predicate)
    {
        var call = Expression.Call(
            null,
            HavingMethodInfo.MakeGenericMethod(typeof(TKey), typeof(TSource)),
            source.Expression,
            predicate);
        return source.Provider.CreateQuery<IGrouping<TKey, TSource>>(call);
    }
}
