using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Kafka.Ksql.Linq.Query.Dsl;

namespace Kafka.Ksql.Linq.Tests.Query.Pipeline;

internal static class TestQueryableExtensions
{
    private static readonly MethodInfo HavingMethodInfo = typeof(TestQueryableExtensions)
        .GetMethod(nameof(Having), BindingFlags.Static | BindingFlags.Public)!
        .GetGenericMethodDefinition();

    private static readonly MethodInfo TumblingMethodInfo = typeof(TestQueryableExtensions)
        .GetMethods(BindingFlags.Static | BindingFlags.Public)
        .Single(m => m.Name == nameof(Tumbling) && m.GetParameters().Length == 3 && !m.IsDefined(typeof(System.Runtime.CompilerServices.ExtensionAttribute), false))
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

    public static IQueryable<TSource> Tumbling<TSource>(
        this IQueryable<TSource> source,
        Expression<Func<TSource, DateTime>> time,
        Windows windows)
    {
        var call = Expression.Call(
            null,
            TumblingMethodInfo.MakeGenericMethod(typeof(TSource)),
            Expression.Quote(time),
            Expression.Constant(windows),
            source.Expression);
        return source.Provider.CreateQuery<TSource>(call);
    }

    public static IQueryable<TSource> Tumbling<TSource>(
        Expression<Func<TSource, DateTime>> time,
        Windows windows,
        IQueryable<TSource> source) => source;
}
