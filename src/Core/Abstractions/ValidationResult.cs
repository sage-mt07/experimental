using System.Collections.Generic;

namespace Kafka.Ksql.Linq.Core.Abstractions;

public class ValidationResult
{
    public bool IsValid { get; set; }
    public List<string> Errors { get; set; } = new();
    public List<string> Warnings { get; set; } = new();
}
