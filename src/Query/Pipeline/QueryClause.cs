using System.Linq.Expressions;

namespace Kafka.Ksql.Linq.Query.Pipeline;

/// <summary>
/// Query clause definition
/// </summary>
internal record QueryClause(
    QueryClauseType Type,
    string Content,
    Expression? SourceExpression = null,
    int Priority = 0)
{
    /// <summary>
    /// Create a required clause
    /// </summary>
    public static QueryClause Required(QueryClauseType type, string content, Expression? sourceExpression = null)
    {
        return new QueryClause(type, content, sourceExpression, 100);
    }

    /// <summary>
    /// Create an optional clause
    /// </summary>
    public static QueryClause Optional(QueryClauseType type, string content, Expression? sourceExpression = null)
    {
        return new QueryClause(type, content, sourceExpression, 50);
    }

    /// <summary>
    /// Determine if clause is empty
    /// </summary>
    public bool IsEmpty => string.IsNullOrWhiteSpace(Content);

    /// <summary>
    /// Determine if clause is valid
    /// </summary>
    public bool IsValid => !IsEmpty;
}

/// <summary>
