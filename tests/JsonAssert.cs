using System;
using System.Linq;
using System.Text.Json;
using Xunit;

namespace Kafka.Ksql.Linq.Tests;

public static class JsonAssert
{
    public static void Equal(string expected, string actual)
    {
        using var docExpected = JsonDocument.Parse(expected);
        using var docActual = JsonDocument.Parse(actual);
        Assert.True(JsonElementEquals(docExpected.RootElement, docActual.RootElement),
            $"Expected JSON does not match actual.\nExpected: {expected}\nActual: {actual}");
    }

    private static bool JsonElementEquals(JsonElement a, JsonElement b)
    {
        if (a.ValueKind != b.ValueKind)
            return false;

        return a.ValueKind switch
        {
            JsonValueKind.Object => ObjectsEqual(a, b),
            JsonValueKind.Array => ArraysEqual(a, b),
            JsonValueKind.String => string.Equals(a.GetString(), b.GetString(), StringComparison.OrdinalIgnoreCase),
            JsonValueKind.Number => a.GetDouble() == b.GetDouble(),
            JsonValueKind.True or JsonValueKind.False => a.GetBoolean() == b.GetBoolean(),
            JsonValueKind.Null => true,
            _ => string.Equals(a.ToString(), b.ToString(), StringComparison.OrdinalIgnoreCase)
        };
    }

    private static bool ObjectsEqual(JsonElement a, JsonElement b)
    {
        var dictA = a.EnumerateObject().ToDictionary(p => p.Name, p => p.Value, StringComparer.OrdinalIgnoreCase);
        var dictB = b.EnumerateObject().ToDictionary(p => p.Name, p => p.Value, StringComparer.OrdinalIgnoreCase);
        if (dictA.Count != dictB.Count)
            return false;
        foreach (var kvp in dictA)
        {
            if (!dictB.TryGetValue(kvp.Key, out var other))
                return false;
            if (!JsonElementEquals(kvp.Value, other))
                return false;
        }
        return true;
    }

    private static bool ArraysEqual(JsonElement a, JsonElement b)
    {
        if (a.GetArrayLength() != b.GetArrayLength())
            return false;
        for (int i = 0; i < a.GetArrayLength(); i++)
        {
            if (!JsonElementEquals(a[i], b[i]))
                return false;
        }
        return true;
    }
}
