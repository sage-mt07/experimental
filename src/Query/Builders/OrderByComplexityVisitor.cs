using System;
using System.Linq;
using System.Linq.Expressions;

namespace Kafka.Ksql.Linq.Query.Builders;
internal class OrderByComplexityVisitor : ExpressionVisitor
{
    public bool HasComplexExpressions { get; private set; }

    protected override Expression VisitBinary(BinaryExpression node)
    {
        // Treat binary expressions as complex
        HasComplexExpressions = true;
        return base.VisitBinary(node);
    }

    protected override Expression VisitConditional(ConditionalExpression node)
    {
        // Treat conditional expressions as complex
        HasComplexExpressions = true;
        return base.VisitConditional(node);
    }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        var methodName = node.Method.Name;

        // Treat non-allowed functions as complex
        var allowedMethods = new[]
        {
            "OrderBy", "OrderByDescending", "ThenBy", "ThenByDescending",
            "RowTime",
            "ToUpper", "ToLower", "Abs", "Year", "Month", "Day"
        };

        if (!allowedMethods.Contains(methodName))
        {
            HasComplexExpressions = true;
        }

        return base.VisitMethodCall(node);
    }
}
