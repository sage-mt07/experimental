using System;
using System.Collections.Generic;

namespace Kafka.Ksql.Linq.Query.Builders.Common;

internal static class KsqlNameUtils
{
    private static readonly HashSet<string> Reserved = new(StringComparer.OrdinalIgnoreCase)
    {
        "KEY",
        "VALUE",
        "WINDOWSTART",
        "WINDOWEND"
    };

    public static string Sanitize(string name)
    {
        return Reserved.Contains(name) ? $"`{name}`" : name;
    }
}
