namespace Kafka.Ksql.Linq.Query.Pipeline;

/// <summary>
/// Enumeration of query clause types
/// </summary>
internal enum QueryClauseType
{
    Select,
    From,
    Join,
    Where,
    GroupBy,
    Having,
    OrderBy,
    Limit
}

