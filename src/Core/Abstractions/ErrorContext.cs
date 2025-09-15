using System;

namespace Kafka.Ksql.Linq.Core.Abstractions;

public class ErrorContext
{
    public Exception Exception { get; set; } = default!;
    public object? OriginalMessage { get; set; }
    public int AttemptCount { get; set; }
    public DateTime FirstAttemptTime { get; set; }
    public DateTime LastAttemptTime { get; set; }
    public string ErrorPhase { get; set; } = string.Empty; // "Deserialization" or "Processing"
}
