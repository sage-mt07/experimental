using Kafka.Ksql.Linq.Query.Builders.Functions;
using System.Linq.Expressions;

namespace Kafka.Ksql.Linq.Query.Builders;
/// <summary>
/// Visitor for detecting aggregate functions
/// </summary>
internal class AggregateDetectionVisitor : ExpressionVisitor
{
    public bool HasAggregates { get; private set; }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        var methodName = node.Method.Name;
        if (KsqlFunctionRegistry.IsAggregateFunction(methodName))
        {
            HasAggregates = true;
        }

        return base.VisitMethodCall(node);
    }
}
