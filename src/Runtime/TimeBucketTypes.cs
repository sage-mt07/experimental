using System;
using System.Collections.Concurrent;
using Kafka.Ksql.Linq.Core.Attributes;

namespace Kafka.Ksql.Linq.Runtime;

internal static class TimeBucketTypes
{
    // Key by base topic alias rather than Type to respect [KsqlTopic("alias")]
    private static readonly ConcurrentDictionary<(string BaseTopic, string Period, string Mode), Type> Map = new();

    public static void RegisterRead(Type baseType, Period period, Type concrete)
    {
        var topic = GetBaseTopic(baseType);
        Map[(topic, period.ToString(), "read")] = concrete;
    }

    public static void RegisterWrite(Type baseType, Period period, Type concrete)
    {
        var topic = GetBaseTopic(baseType);
        Map[(topic, period.ToString(), "write")] = concrete;
    }

    public static Type? ResolveRead(Type baseType, Period period)
    {
        var topic = GetBaseTopic(baseType);
        return Map.TryGetValue((topic, period.ToString(), "read"), out var t) ? t : baseType;
    }

    public static Type? ResolveWrite(Type baseType, Period period)
    {
        var topic = GetBaseTopic(baseType);
        return Map.TryGetValue((topic, period.ToString(), "write"), out var t) ? t : baseType;
    }

    // Resolve base topic respecting [KsqlTopic("alias")]
    public static string GetBaseTopic(Type baseType)
    {
        var attr = (KsqlTopicAttribute?)Attribute.GetCustomAttribute(baseType, typeof(KsqlTopicAttribute));
        return (attr?.Name ?? baseType.Name).ToLowerInvariant();
    }

    // Build physical topic name per chart.md: {topic}_{period}_live (1s uses _1s_final)
    public static string GetTopicName(Type baseType, Period period)
    {
        var topic = GetBaseTopic(baseType);
        if (period.Unit == PeriodUnit.Seconds && period.Value == 1)
            return $"{topic}_1s_final";
        return $"{topic}_{period}_live";
    }
}
