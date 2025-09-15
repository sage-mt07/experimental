namespace Kafka.Ksql.Linq.Query.Abstractions;
/// <summary>
/// Enumeration of builder kinds.
/// </summary>
public enum KsqlBuilderType
{
    Select,
    Where,
    GroupBy,
    Having,
    Join,
    Projection,
    OrderBy
}
