using System.Linq.Expressions;

namespace Kafka.Ksql.Linq.Query.Abstractions;
/// <summary>
/// Common interface for KSQL syntax builders.
/// Rationale: unify builder classes and clarify responsibilities.
/// </summary>
public interface IKsqlBuilder
{
    /// <summary>
    /// Build a KSQL statement from an expression tree.
    /// </summary>
    /// <param name="expression">Target expression tree</param>
    /// <returns>KSQL statement string</returns>
    string Build(Expression expression);

    /// <summary>
    /// Identifies the builder type.
    /// </summary>
    KsqlBuilderType BuilderType { get; }
}
