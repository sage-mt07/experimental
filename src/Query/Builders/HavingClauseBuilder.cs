using Kafka.Ksql.Linq.Query.Abstractions;
using Kafka.Ksql.Linq.Query.Builders.Common;
using System;
using System.Linq.Expressions;

namespace Kafka.Ksql.Linq.Query.Builders;

/// <summary>
/// Builder for HAVING clause content.
/// Rationale: separation-of-concerns; generate only aggregate condition content without keywords.
/// Example output: "SUM(amount) > 100 AND COUNT(*) > 5" (excluding HAVING)
/// </summary>
internal class HavingClauseBuilder : BuilderBase
{
    public override KsqlBuilderType BuilderType => KsqlBuilderType.Having;

    protected override KsqlBuilderType[] GetRequiredBuilderTypes()
    {
        return Array.Empty<KsqlBuilderType>(); // No dependency on other builders
    }

    protected override string BuildInternal(Expression expression)
    {
        var visitor = new HavingExpressionVisitor();
        visitor.Visit(expression);
        return visitor.GetResult();
    }

    protected override void ValidateBuilderSpecific(Expression expression)
    {
        // HAVING-specific validation
        BuilderValidation.ValidateNoNestedAggregates(expression);
        ValidateRequiresAggregateOrGroupByColumn(expression);
    }

    /// <summary>
    /// Allow only aggregate functions or GROUP BY columns in HAVING clause
    /// </summary>
    private static void ValidateRequiresAggregateOrGroupByColumn(Expression expression)
    {
        var visitor = new HavingValidationVisitor();
        visitor.Visit(expression);

        if (visitor.HasInvalidReferences)
        {
            throw new InvalidOperationException(
                "HAVING clause can only reference aggregate functions or columns in GROUP BY clause");
        }
    }
}
