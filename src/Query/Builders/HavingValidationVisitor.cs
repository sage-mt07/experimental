using Kafka.Ksql.Linq.Query.Builders.Functions;
using System.Linq.Expressions;

namespace Kafka.Ksql.Linq.Query.Builders;

/// <summary>
/// Validation visitor for HAVING clause.
/// </summary>
internal class HavingValidationVisitor : ExpressionVisitor
{
    public bool HasInvalidReferences { get; private set; }
    private bool _insideAggregateFunction;

    protected override Expression VisitMember(MemberExpression node)
    {
        // Member access outside aggregate functions must be a GROUP BY column
        // Simplified here; actual implementation should check against the list of GROUP BY columns
        if (!_insideAggregateFunction && node.Expression is ParameterExpression)
        {
            // This is where you'd match against actual GROUP BY columns (simplified)
            // In a full implementation, compare with the list of columns used in GROUP BY
        }

        return base.VisitMember(node);
    }


    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        var wasInside = _insideAggregateFunction;
        if (KsqlFunctionRegistry.IsAggregateFunction(node.Method.Name))
        {
            _insideAggregateFunction = true;
        }

        var result = base.VisitMethodCall(node);
        _insideAggregateFunction = wasInside;
        return result;
    }
}
