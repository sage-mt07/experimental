using Kafka.Ksql.Linq.Query.Abstractions;
using Kafka.Ksql.Linq.Query.Builders.Common;
using System;
using System.Linq.Expressions;

namespace Kafka.Ksql.Linq.Query.Builders;

/// <summary>
/// Builder for JOIN clauses (limited to 3 tables).
/// Rationale: separation-of-concerns; outputs a complete JOIN statement including keywords.
/// Example output: "JOIN table2 t2 ON t1.key = t2.key"
/// </summary>
internal class JoinClauseBuilder : BuilderBase
{
    public override KsqlBuilderType BuilderType => KsqlBuilderType.Join;

    protected override KsqlBuilderType[] GetRequiredBuilderTypes()
    {
        return Array.Empty<KsqlBuilderType>(); // No dependency on other builders
    }

    protected override string BuildInternal(Expression expression)
    {
        // Pre-check join limitations
        JoinLimitationEnforcer.ValidateJoinExpression(expression);

        var visitor = new JoinExpressionVisitor();
        visitor.Visit(expression);

        var result = visitor.GetResult();

        if (string.IsNullOrWhiteSpace(result))
        {
            return "/* UNSUPPORTED JOIN PATTERN */";
        }

        return result;
    }

    protected override void ValidateBuilderSpecific(Expression expression)
    {
        // JOIN-specific validation
        ValidateJoinStructure(expression);
        ValidateJoinTypes(expression);
    }

    /// <summary>
    /// Validate JOIN structure
    /// </summary>
    private static void ValidateJoinStructure(Expression expression)
    {
        var joinCall = FindJoinCall(expression);
        if (joinCall == null)
        {
            throw new InvalidOperationException("Expression does not contain a valid JOIN operation");
        }

        // Check number of JOIN arguments (outer, inner, outerKeySelector, innerKeySelector, resultSelector)
        if (joinCall.Arguments.Count < 4)
        {
            throw new InvalidOperationException(
                $"JOIN operation requires at least 4 arguments, but got {joinCall.Arguments.Count}");
        }
    }

    /// <summary>
    /// Validate JOIN types
    /// </summary>
    private static void ValidateJoinTypes(Expression expression)
    {
        // Additional check even though JoinLimitationEnforcer already validated
        var joinCall = FindJoinCall(expression);
        if (joinCall?.Method.Name != "Join")
        {
            throw new InvalidOperationException(
                "Only INNER JOIN is supported. Use Join() method for INNER JOIN operations.");
        }
    }

    /// <summary>
    /// Find JOIN call
    /// </summary>
    private static MethodCallExpression? FindJoinCall(Expression expr)
    {
        return expr switch
        {
            MethodCallExpression mce when mce.Method.Name == "Join" => mce,
            LambdaExpression le => FindJoinCall(le.Body),
            UnaryExpression ue => FindJoinCall(ue.Operand),
            InvocationExpression ie => FindJoinCall(ie.Expression),
            _ => null
        };
    }
}
