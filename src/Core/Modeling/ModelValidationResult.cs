using System;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Ksql.Linq.Core.Modeling;
internal class ModelValidationResult
{
    public bool HasErrors { get; set; }
    public Dictionary<Type, List<string>> EntityErrors { get; set; } = new();
    public Dictionary<Type, List<string>> EntityWarnings { get; set; } = new();

    public bool IsValid => !HasErrors;

    public string GetSummary()
    {
        if (IsValid && !EntityWarnings.Any())
            return "Model validation passed without issues";

        var summary = new List<string>();

        if (HasErrors)
        {
            summary.Add($"❌ Model validation failed with {EntityErrors.Sum(x => x.Value.Count)} errors:");
            foreach (var (entityType, errors) in EntityErrors)
            {
                summary.Add($"  {entityType.Name}:");
                foreach (var error in errors)
                {
                    summary.Add($"    - {error}");
                }
            }
        }

        if (EntityWarnings.Any())
        {
            summary.Add($"⚠️ Model validation completed with {EntityWarnings.Sum(x => x.Value.Count)} warnings:");
            foreach (var (entityType, warnings) in EntityWarnings)
            {
                summary.Add($"  {entityType.Name}:");
                foreach (var warning in warnings)
                {
                    summary.Add($"    - {warning}");
                }
            }
        }

        return string.Join(Environment.NewLine, summary);
    }
}
