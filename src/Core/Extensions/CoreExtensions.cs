using Kafka.Ksql.Linq.Core.Abstractions;
using System;
using System.Reflection;

namespace Kafka.Ksql.Linq.Core.Extensions;
internal static class CoreExtensions
{
    /// <summary>
    /// EntityModel helper extensions.
    /// </summary>
    public static string GetTopicName(this EntityModel entityModel)
    {
        return (entityModel.TopicName ?? entityModel.EntityType.Name).ToLowerInvariant();
    }

    public static bool HasKeys(this EntityModel entityModel)
    {
        return entityModel.KeyProperties.Length > 0;
    }

    public static bool IsCompositeKey(this EntityModel entityModel)
    {
        return entityModel.KeyProperties.Length > 1;
    }

    public static PropertyInfo[] GetOrderedKeyProperties(this EntityModel entityModel)
    {
        return entityModel.KeyProperties;
    }

    public static PropertyInfo[] GetSerializableProperties(this EntityModel entityModel)
    {
        return entityModel.AllProperties;
    }

    /// <summary>
    /// Type helper extensions.
    /// </summary>
    public static bool IsKafkaEntity(this Type type)
    {
        return type.IsClass && !type.IsAbstract;
    }

    public static string GetKafkaTopicName(this Type type)
    {
        return type.Name.ToLowerInvariant();
    }

    public static bool HasKafkaKeys(this Type type)
    {
        return false;
    }

    /// <summary>
    /// PropertyInfo helper extensions.
    /// </summary>
    public static bool IsKafkaIgnored(this PropertyInfo property)
    {
        return false;
    }

    public static bool IsKafkaKey(this PropertyInfo property)
    {
        return false;
    }

    public static int GetKeyOrder(this PropertyInfo property)
    {
        return 0;
    }

    public static bool IsNullableProperty(this PropertyInfo property)
    {
        var propertyType = property.PropertyType;

        // Nullable value types
        if (Nullable.GetUnderlyingType(propertyType) != null)
            return true;

        // Value types are non-nullable by default
        if (propertyType.IsValueType)
            return false;

        // Reference types - check nullable context
        try
        {
            var nullabilityContext = new NullabilityInfoContext();
            var nullabilityInfo = nullabilityContext.Create(property);
            return nullabilityInfo.WriteState == NullabilityState.Nullable ||
                   nullabilityInfo.ReadState == NullabilityState.Nullable;
        }
        catch
        {
            // Fallback: assume reference types are nullable
            return !propertyType.IsValueType;
        }
    }
}
