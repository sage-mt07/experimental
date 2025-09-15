using Kafka.Ksql.Linq.Query.Builders.Functions;
using System.Linq.Expressions;

namespace Kafka.Ksql.Linq.Query.Builders;
/// <summary>
/// Visitor for detecting non-aggregate columns
/// </summary>
internal class NonAggregateColumnVisitor : ExpressionVisitor
{
    public bool HasNonAggregateColumns { get; private set; }
    private bool _insideAggregateFunction;

    protected override Expression VisitMember(MemberExpression node)
    {
        if (!_insideAggregateFunction && node.Expression is ParameterExpression)
        {
            HasNonAggregateColumns = true;
        }

        return base.VisitMember(node);
    }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        var methodName = node.Method.Name;
        var wasInsideAggregate = _insideAggregateFunction;

        if (KsqlFunctionRegistry.IsAggregateFunction(methodName))
        {
            _insideAggregateFunction = true;
        }

        var result = base.VisitMethodCall(node);
        _insideAggregateFunction = wasInsideAggregate;

        return result;
    }
}
