namespace Kafka.Ksql.Linq.Core.Abstractions;

public enum ErrorAction
{
    /// <summary>
    /// Skip the error record and continue processing
    /// </summary>
    Skip,

    /// <summary>
    /// Retry the specified number of times
    /// </summary>
    Retry,

    /// <summary>
    /// Send to the Dead Letter Queue
    /// </summary>
    DLQ
}
