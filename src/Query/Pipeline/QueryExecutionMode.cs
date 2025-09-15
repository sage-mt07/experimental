namespace Kafka.Ksql.Linq.Query.Pipeline;
/// <summary>
/// Query execution mode
/// Rationale: distinguish Pull queries (one-shot) from Push queries (streaming)
/// </summary>
public enum QueryExecutionMode
{
    /// <summary>
    /// Execution mode not explicitly specified.
    /// </summary>
    Unspecified,
    /// <summary>
    /// Pull Query - executes once
    /// </summary>
    PullQuery,

    /// <summary>
    /// Push Query - continuous streaming query
    /// </summary>
    PushQuery
}
