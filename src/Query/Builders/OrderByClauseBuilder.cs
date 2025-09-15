using Kafka.Ksql.Linq.Query.Abstractions;
using Kafka.Ksql.Linq.Query.Builders.Common;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Kafka.Ksql.Linq.Query.Builders;

/// <summary>
/// Builder for ORDER BY clause content.
/// Rationale: separation-of-concerns; generate only sorting content without keywords.
/// Example output: "col1 ASC, col2 DESC" (excluding ORDER BY)
/// </summary>
internal class OrderByClauseBuilder : BuilderBase
{
    public override KsqlBuilderType BuilderType => KsqlBuilderType.OrderBy;

    protected override KsqlBuilderType[] GetRequiredBuilderTypes()
    {
        return Array.Empty<KsqlBuilderType>(); // No dependency on other builders
    }

    protected override string BuildInternal(Expression expression)
    {
        var visitor = new OrderByExpressionVisitor();
        visitor.Visit(expression);

        var result = visitor.GetResult();

        if (string.IsNullOrWhiteSpace(result))
        {
            throw new InvalidOperationException("Unable to extract ORDER BY columns from expression");
        }

        return result;
    }

    protected override void ValidateBuilderSpecific(Expression expression)
    {
        // ORDER BY-specific validation
        ValidateOrderByLimitations(expression);
        ValidateOrderByColumns(expression);
    }

    /// <summary>
    /// Check ORDER BY restrictions
    /// </summary>
    private static void ValidateOrderByLimitations(Expression expression)
    {
        // ORDER BY restrictions in KSQL
        Console.WriteLine("[KSQL-LINQ INFO] ORDER BY in KSQL is limited to Pull Queries and specific scenarios. " +
                         "Push Queries (streaming) do not guarantee order due to distributed processing.");

        foreach (var selector in ExtractKeySelectors(expression))
        {
            var visitor = new OrderByComplexityVisitor();
            visitor.Visit(selector.Body);
            if (visitor.HasComplexExpressions)
            {
                throw new InvalidOperationException(
                    "ORDER BY in KSQL should use simple column references. " +
                    "Complex expressions in ORDER BY may not be supported.");
            }
        }
    }

    /// <summary>
    /// Check ORDER BY column count limits
    /// </summary>
    private static void ValidateOrderByColumns(Expression expression)
    {
        var visitor = new OrderByColumnCountVisitor();
        visitor.Visit(expression);

        const int maxColumns = 5; // recommended limit in KSQL
        if (visitor.ColumnCount > maxColumns)
        {
            throw new InvalidOperationException(
                $"ORDER BY supports maximum {maxColumns} columns for optimal performance. " +
                $"Found {visitor.ColumnCount} columns. Consider reducing sort columns.");
        }
    }

    /// <summary>
    /// Extract key selectors used in ORDER BY
    /// </summary>
    private static IEnumerable<LambdaExpression> ExtractKeySelectors(Expression expression)
    {
        if (expression is MethodCallExpression mc)
        {
            if (mc.Object != null)
            {
                foreach (var sel in ExtractKeySelectors(mc.Object))
                    yield return sel;
            }

            if (mc.Method.Name is "OrderBy" or "OrderByDescending" or "ThenBy" or "ThenByDescending")
            {
                if (mc.Arguments.Count >= 2)
                {
                    var lambda = ExtractLambda(mc.Arguments[1]);
                    if (lambda != null)
                        yield return lambda;
                }
            }
        }
    }

    private static LambdaExpression? ExtractLambda(Expression expr)
    {
        return expr switch
        {
            UnaryExpression { Operand: LambdaExpression lambda } => lambda,
            LambdaExpression lambda => lambda,
            _ => null
        };
    }
}
