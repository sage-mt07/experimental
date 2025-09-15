using System;
using System.Collections.Generic;

namespace Kafka.Ksql.Linq.Query.Pipeline;

internal record QueryMetadata(
    DateTime CreatedAt,
    string Category,
    string? BaseObject = null,
    Dictionary<string, object>? Properties = null)
{
    public int? GraceSeconds { get; init; }
    /// <summary>
    /// Add property
    /// </summary>
    public QueryMetadata WithProperty(string key, object value)
    {
        var newProperties = new Dictionary<string, object>(Properties ?? new Dictionary<string, object>())
        {
            [key] = value
        };
        return this with { Properties = newProperties };
    }

    /// <summary>
    /// Get property
    /// </summary>
    public T? GetProperty<T>(string key)
    {
        if (Properties?.TryGetValue(key, out var value) == true && value is T typedValue)
        {
            return typedValue;
        }
        return default;
    }
}

