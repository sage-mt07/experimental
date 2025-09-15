using Kafka.Ksql.Linq.Query.Abstractions;
using Kafka.Ksql.Linq.Query.Builders.Common;
using System;
using System.Linq.Expressions;

namespace Kafka.Ksql.Linq.Query.Builders;

/// <summary>
/// Builder for WHERE clause content.
/// Rationale: separation-of-concerns; generate only predicate content without keywords.
/// Example output: "condition1 AND condition2" (excluding WHERE)
/// </summary>
internal class WhereClauseBuilder : BuilderBase
{
    public override KsqlBuilderType BuilderType => KsqlBuilderType.Where;
    private readonly System.Collections.Generic.IDictionary<string, string>? _paramToAlias;

    public WhereClauseBuilder() { }
    public WhereClauseBuilder(System.Collections.Generic.IDictionary<string, string> paramToAlias)
    {
        _paramToAlias = paramToAlias;
    }

    protected override KsqlBuilderType[] GetRequiredBuilderTypes()
    {
        return Array.Empty<KsqlBuilderType>(); // No dependency on other builders
    }

    protected override string BuildInternal(Expression expression)
    {
        var visitor = _paramToAlias == null ? new WhereExpressionVisitor() : new WhereExpressionVisitor(_paramToAlias);
        visitor.Visit(expression);
        return visitor.GetResult();
    }

    protected override void ValidateBuilderSpecific(Expression expression)
    {
        // WHERE-specific validation
        ValidateNoAggregateInWhere(expression);
        ValidateNoSelectStatements(expression);
    }

    /// <summary>
    /// Disallow aggregate functions in WHERE clause
    /// </summary>
    private static void ValidateNoAggregateInWhere(Expression expression)
    {
        var visitor = new AggregateDetectionVisitor();
        visitor.Visit(expression);

        if (visitor.HasAggregates)
        {
            throw new InvalidOperationException(
                "Aggregate functions are not allowed in WHERE clause. Use HAVING clause instead.");
        }
    }

    /// <summary>
    /// Disallow SELECT statements within WHERE clause
    /// </summary>
    private static void ValidateNoSelectStatements(Expression expression)
    {
        // Detect basic subquery patterns
        var expressionString = expression.ToString().ToUpper();
        if (expressionString.Contains("SELECT"))
        {
            throw new InvalidOperationException(
                "Subqueries are not supported in WHERE clause in KSQL");
        }
    }

    /// <summary>
    /// Build only conditions (without WHERE prefix)
    /// </summary>
    public string BuildCondition(Expression expression)
    {
        return BuildInternal(expression);
    }
}
