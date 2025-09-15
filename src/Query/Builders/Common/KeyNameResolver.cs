using Kafka.Ksql.Linq.Core.Extensions;
using Kafka.Ksql.Linq.Core.Modeling;
using System;
using System.Collections.Concurrent;

namespace Kafka.Ksql.Linq.Query.Builders.Common;

internal static class KeyNameResolver
{
    private static readonly ConcurrentDictionary<Type, string> Cache = new();

    public static string GetKeyPrefix(Type type)
    {
        return Cache.GetOrAdd(type, t =>
        {
            var builder = new ModelBuilder();
            builder.AddEntityModel(t);
            var model = builder.GetEntityModel(t)!;
            return $"{model.GetTopicName()}.key";
        });
    }
}
