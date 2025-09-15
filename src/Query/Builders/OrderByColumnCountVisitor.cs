using System.Linq.Expressions;

namespace Kafka.Ksql.Linq.Query.Builders;
/// <summary>
/// Visitor counting ORDER BY columns
/// </summary>
internal class OrderByColumnCountVisitor : ExpressionVisitor
{
    public int ColumnCount { get; private set; }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        var methodName = node.Method.Name;

        // Increment count when encountering ORDER BY-related methods
        if (methodName is "OrderBy" or "OrderByDescending" or "ThenBy" or "ThenByDescending")
        {
            ColumnCount++;
        }

        return base.VisitMethodCall(node);
    }
}
