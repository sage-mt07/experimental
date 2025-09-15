namespace Kafka.Ksql.Linq.Query.Dsl;

/// <summary>
/// Represents the order of DSL method calls for building a KSQL query.
/// </summary>
internal enum QueryBuildStage
{
    Start,
    From,
    Join,
    Where,
    GroupBy,
    Having,
    Select
}
