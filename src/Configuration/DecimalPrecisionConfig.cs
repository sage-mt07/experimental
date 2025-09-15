using System.Collections.Generic;
using System.Reflection;

namespace Kafka.Ksql.Linq.Configuration;

/// <summary>
/// Global decimal precision settings used across query generation and schema definitions.
/// </summary>
public static class DecimalPrecisionConfig
{
    public static int DecimalPrecision { get; private set; } = 18;
    public static int DecimalScale { get; private set; } = 2;

    private static Dictionary<string, (int Precision, int Scale)> _overrides = new();

    public static void Configure(int precision, int scale, Dictionary<string, Dictionary<string, KsqlDslOptions.DecimalSetting>>? overrides)
    {
        DecimalPrecision = precision;
        DecimalScale = scale;
        _overrides = new();
        if (overrides != null)
        {
            foreach (var (entity, props) in overrides)
            {
                foreach (var (prop, val) in props)
                {
                    _overrides[$"{entity}.{prop}"] = (val.Precision, val.Scale);
                }
            }
        }
    }

    /// <summary>
    /// Configure only per-property decimal overrides while keeping global defaults.
    /// Use this when global precision/scale options are not provided.
    /// </summary>
    public static void Configure(Dictionary<string, Dictionary<string, KsqlDslOptions.DecimalSetting>>? overrides)
    {
        DecimalPrecision = 18;
        DecimalScale = 2;
        _overrides = new();
        if (overrides == null) return;
        foreach (var (entity, props) in overrides)
        {
            foreach (var (prop, val) in props)
            {
                _overrides[$"{entity}.{prop}"] = (val.Precision, val.Scale);
            }
        }
    }

    public static void Override(PropertyInfo property, int precision, int scale)
    {
        var key = $"{property.DeclaringType?.Name}.{property.Name}";
        _overrides[key] = (precision, scale);
    }

    public static int ResolvePrecision(int? precision, PropertyInfo? property = null)
    {
        if (property != null)
        {
            var key = $"{property.DeclaringType?.Name}.{property.Name}";
            if (_overrides.TryGetValue(key, out var ov))
                return ov.Precision;
        }
        return precision ?? DecimalPrecision;
    }

    public static int ResolveScale(int? scale, PropertyInfo? property = null)
    {
        if (property != null)
        {
            var key = $"{property.DeclaringType?.Name}.{property.Name}";
            if (_overrides.TryGetValue(key, out var ov))
                return ov.Scale;
        }
        return scale ?? DecimalScale;
    }
}
