using System;
using System.Linq;

namespace Kafka.Ksql.Linq.Query.Pipeline;

internal static class WindowValidator
{
    public static void Validate(ExpressionAnalysisResult result)
    {
        if (result == null) throw new ArgumentNullException(nameof(result));
        if (result.Windows.Count == 0)
            return;
        if (!result.BaseUnitSeconds.HasValue)
            throw new InvalidOperationException("Base unit is required for tumbling windows.");

        var baseUnit = result.BaseUnitSeconds.Value;

        if (60 % baseUnit != 0)
            throw new InvalidOperationException("Base unit must divide 60 seconds.");

        var ordered = result.Windows.OrderBy(ToSeconds).ToList();
        foreach (var w in ordered)
        {
            var seconds = ToSeconds(w);

            if (seconds % baseUnit != 0)
                throw new InvalidOperationException($"Window {w} must be a multiple of base {baseUnit}s.");

            if (seconds >= 60 && seconds % 60 != 0)
                throw new InvalidOperationException("Windows ≥ 1 minute must be whole-minute multiples.");
        }

        var grace = result.GraceSeconds ?? 0;
        foreach (var w in ordered)
        {
            grace++;
            if (result.GracePerTimeframe.TryGetValue(w, out var g))
            {
                if (g != grace)
                    throw new InvalidOperationException($"Window {w} grace must be parent grace + 1s.");
            }
            else
            {
                result.GracePerTimeframe[w] = grace;
            }
        }
    }

    private static int ToSeconds(string w)
    {
        if (w.EndsWith("mo", StringComparison.OrdinalIgnoreCase))
            return int.Parse(w[..^2]) * 30 * 24 * 3600;
        if (w.EndsWith("wk", StringComparison.OrdinalIgnoreCase))
            return int.Parse(w[..^2]) * 7 * 24 * 3600;
        var unit = w[^1];
        var value = int.Parse(w[..^1]);
        return unit switch
        {
            's' => value,
            'm' => value * 60,
            'h' => value * 3600,
            'd' => value * 86400,
            _ => value
        };
    }
}
