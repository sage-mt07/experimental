using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Kafka.Ksql.Linq.Core.Extensions;

internal static class ConfigLoggingExtensions
{
    private static readonly string[] SensitiveKeys = new[] { "password", "secret", "token", "apikey" };

    private static bool IsSensitive(string key)
        => SensitiveKeys.Any(k => key.IndexOf(k, StringComparison.OrdinalIgnoreCase) >= 0);

    private static IEnumerable<(string Key, object Value)> ReflectProps(object config)
    {
        var type = config.GetType();
        var props = type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanRead && p.GetIndexParameters().Length == 0)
            .OrderBy(p => p.Name);

        foreach (var p in props)
        {
            object? val;
            try { val = p.GetValue(config); }
            catch { continue; }
            if (val is null) continue;
            yield return (p.Name, val!);
        }
    }

    public static void LogClientConfig(this ILogger? logger, string label, object config, IDictionary<string, string>? additional = null)
    {
        if (logger == null || !logger.IsEnabled(LogLevel.Information) || config == null)
            return;

        var entries = new List<string>();
        foreach (var (key, value) in ReflectProps(config))
        {
            var str = value is Enum ? value.ToString() : value as string ?? value.ToString();
            if (IsSensitive(key)) str = "******";
            entries.Add($"{key}={str}");
        }

        if (additional != null)
        {
            foreach (var kv in additional)
            {
                var key = $"additional.{kv.Key}";
                var val = IsSensitive(kv.Key) ? "******" : kv.Value ?? string.Empty;
                entries.Add($"{key}={val}");
            }
        }

        var flat = string.Join(", ", entries);
        logger.LogInformation("{Label} config: {Settings}", label, flat);
    }
}

