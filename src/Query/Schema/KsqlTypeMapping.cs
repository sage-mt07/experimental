using Kafka.Ksql.Linq.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Ksql.Linq.Query.Schema;

internal static class KsqlTypeMapping
{
    private static bool TryGetDictionaryTypes(Type t, out Type? keyType, out Type? valType)
    {
        keyType = null;
        valType = null;

        if (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(Dictionary<,>))
        {
            var args = t.GetGenericArguments();
            keyType = args[0];
            valType = args[1];
            return true;
        }

        var idict = t.GetInterfaces()
            .FirstOrDefault(x => x.IsGenericType && x.GetGenericTypeDefinition() == typeof(IDictionary<,>));

        if (idict != null)
        {
            var args = idict.GetGenericArguments();
            keyType = args[0];
            valType = args[1];
            return true;
        }

        return false;
    }

    public static string MapToKsqlType(Type propertyType, System.Reflection.PropertyInfo? propertyInfo, int? precision = null, int? scale = null)
    {
        if (TryGetDictionaryTypes(propertyType, out var keyT, out var valT))
        {
            if (keyT != typeof(string))
                throw new NotSupportedException("ksqlDB MAP key must be STRING.");

            if (valT != typeof(string))
                throw new NotSupportedException("Only Dictionary<string, string> is supported currently.");

            return "MAP<STRING, STRING>";
        }

        var underlyingType = Nullable.GetUnderlyingType(propertyType) ?? propertyType;

        return underlyingType switch
        {
            Type t when t == typeof(int) => "INT",
            Type t when t == typeof(short) => "INT",
            Type t when t == typeof(long) => "BIGINT",
            Type t when t == typeof(double) => "DOUBLE",
            Type t when t == typeof(float) => "DOUBLE",
            Type t when t == typeof(decimal) => $"DECIMAL({DecimalPrecisionConfig.ResolvePrecision(precision, propertyInfo)}, {DecimalPrecisionConfig.ResolveScale(scale, propertyInfo)})",
            Type t when t == typeof(string) => "VARCHAR",
            Type t when t == typeof(char) => "VARCHAR",
            Type t when t == typeof(bool) => "BOOLEAN",
            Type t when t == typeof(DateTime) => "TIMESTAMP",
            Type t when t == typeof(DateTimeOffset) => "TIMESTAMP",
            Type t when t == typeof(Guid) => "VARCHAR",
            Type t when t == typeof(byte[]) => "BYTES",
            _ when underlyingType.IsEnum => throw new NotSupportedException($"Type '{underlyingType.Name}' is not supported."),
            _ when !underlyingType.IsPrimitive && underlyingType != typeof(string) && underlyingType != typeof(char) && underlyingType != typeof(Guid) && underlyingType != typeof(byte[]) => throw new NotSupportedException($"Type '{underlyingType.Name}' is not supported."),
            _ => throw new NotSupportedException($"Type '{underlyingType.Name}' is not supported.")
        };
    }
}
