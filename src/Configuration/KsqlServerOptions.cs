using System;
using System.Linq;

namespace Kafka.Ksql.Linq.Configuration;

public class KsqlServerOptions
{
    private const string DefaultServiceId = "ksql_service_1";
    private const string DefaultQueryPrefix = "query_";

    public string ServiceId { get; init; } = ResolveServiceId();
    public string PersistentQueryPrefix { get; init; } = ResolveQueryPrefix();

    private static string ResolveServiceId()
    {
        var env = Environment.GetEnvironmentVariable("KSQL_SERVICE_ID");
        return Sanitize(env, DefaultServiceId, ensureTrailingUnderscore: false);
    }

    private static string ResolveQueryPrefix()
    {
        var env = Environment.GetEnvironmentVariable("KSQL_PERSISTENT_PREFIX");
        return Sanitize(env, DefaultQueryPrefix, ensureTrailingUnderscore: true);
    }

    private static string Sanitize(string? value, string fallback, bool ensureTrailingUnderscore)
    {
        if (string.IsNullOrWhiteSpace(value))
            return ensureTrailingUnderscore ? EnsureSuffix(fallback) : fallback;

        var cleaned = new string(value.Trim()
            .ToLowerInvariant()
            .Select(ch => char.IsLetterOrDigit(ch) || ch == '_' ? ch : '_')
            .ToArray());

        if (string.IsNullOrWhiteSpace(cleaned))
            cleaned = fallback;

        if (!char.IsLetter(cleaned[0]) && cleaned[0] != '_')
            cleaned = $"ksql_{cleaned}";

        cleaned = cleaned.Trim('_');
        if (string.IsNullOrWhiteSpace(cleaned))
            cleaned = fallback;

        return ensureTrailingUnderscore ? EnsureSuffix(cleaned) : cleaned;
    }

    private static string EnsureSuffix(string value)
        => value.EndsWith("_", StringComparison.Ordinal) ? value : value + "_";
}
