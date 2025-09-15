using Kafka.Ksql.Linq.Query.Builders.Common;
using Kafka.Ksql.Linq.Query.Builders.Functions;
using System;
using System.Linq.Expressions;

namespace Kafka.Ksql.Linq.Query.Builders;

/// <summary>
/// ExpressionVisitor specialized for HAVING clause.
/// </summary>
internal class HavingExpressionVisitor : ExpressionVisitor
{
    private string _result = string.Empty;

    public string GetResult()
    {
        return _result;
    }

    protected override Expression VisitBinary(BinaryExpression node)
    {
        var left = ProcessExpression(node.Left);
        var right = ProcessExpression(node.Right);
        var varoperator = GetSqlOperator(node.NodeType);

        _result = $"({left} {varoperator} {right})";
        return node;
    }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        var methodName = node.Method.Name;

        // Handle aggregate functions
        if (KsqlFunctionRegistry.IsAggregateFunction(methodName))
        {
            _result = ProcessAggregateFunction(node);
            return node;
        }

        // Other functions
        _result = KsqlFunctionTranslator.TranslateMethodCall(node);
        return node;
    }

    protected override Expression VisitMember(MemberExpression node)
    {
        // Referencing GROUP BY columns
        _result = node.Member.Name;
        return node;
    }

    protected override Expression VisitConstant(ConstantExpression node)
    {
        _result = SafeToString(node.Value);
        return node;
    }

    protected override Expression VisitUnary(UnaryExpression node)
    {
        switch (node.NodeType)
        {
            case ExpressionType.Not:
                var operand = ProcessExpression(node.Operand);
                _result = $"NOT ({operand})";
                break;

            case ExpressionType.Convert:
            case ExpressionType.ConvertChecked:
                Visit(node.Operand);
                break;

            default:
                Visit(node.Operand);
                break;
        }

        return node;
    }

    /// <summary>
    /// Handle aggregate functions
    /// </summary>
    private string ProcessAggregateFunction(MethodCallExpression methodCall)
    {
        var methodName = methodCall.Method.Name;
        var ksqlFunction = TransformAggregateMethodName(methodName);

        // Special handling for Count function
        if (methodName == "Count")
        {
            return ProcessCountFunction(methodCall, ksqlFunction);
        }

        // Other aggregate functions
        return ProcessStandardAggregateFunction(methodCall, ksqlFunction);
    }

    /// <summary>
    /// Handle Count function
    /// </summary>
    private string ProcessCountFunction(MethodCallExpression methodCall, string ksqlFunction)
    {
        // Count() - no arguments
        if (methodCall.Arguments.Count == 0)
        {
            return "COUNT(*)";
        }

        // Count(selector) - when using a lambda
        if (methodCall.Arguments.Count == 1 && methodCall.Arguments[0] is LambdaExpression)
        {
            return "COUNT(*)";
        }

        // Count(source) - specifying source
        if (methodCall.Arguments.Count == 1)
        {
            return "COUNT(*)";
        }

        // Count(source, predicate) - conditional count (not supported in KSQL)
        if (methodCall.Arguments.Count == 2)
        {
            throw new InvalidOperationException(
                "Conditional Count is not supported in KSQL HAVING clause. Use WHERE clause instead.");
        }

        return "COUNT(*)";
    }

    /// <summary>
    /// Handle standard aggregate functions
    /// </summary>
    private string ProcessStandardAggregateFunction(MethodCallExpression methodCall, string ksqlFunction)
    {
        // For instance methods (g.Sum(x => x.Amount))
        if (methodCall.Arguments.Count == 1 && methodCall.Arguments[0] is LambdaExpression lambda)
        {
            var columnName = ExtractColumnFromLambda(lambda);
            return $"{ksqlFunction}({columnName})";
        }

        // For static methods (extension method)
        if (methodCall.Method.IsStatic && methodCall.Arguments.Count >= 2)
        {
            var staticLambda = ExtractLambda(methodCall.Arguments[1]);
            if (staticLambda != null)
            {
                var columnName = ExtractColumnFromLambda(staticLambda);
                return $"{ksqlFunction}({columnName})";
            }
        }

        // For object methods
        if (methodCall.Object is MemberExpression objMember)
        {
            return $"{ksqlFunction}({objMember.Member.Name})";
        }

        // Fallback
        return $"{ksqlFunction}(*)";
    }

    /// <summary>
    /// Extract column name from lambda
    /// </summary>
    private string ExtractColumnFromLambda(LambdaExpression lambda)
    {
        return lambda.Body switch
        {
            MemberExpression member => member.Member.Name,
            UnaryExpression unary when unary.Operand is MemberExpression memberInner => memberInner.Member.Name,
            _ => throw new InvalidOperationException($"Cannot extract column name from lambda: {lambda}")
        };
    }

    /// <summary>
    /// Extract lambda expression
    /// </summary>
    private static LambdaExpression? ExtractLambda(Expression expr)
    {
        return expr switch
        {
            LambdaExpression lambda => lambda,
            UnaryExpression { Operand: LambdaExpression lambda } => lambda,
            _ => null
        };
    }

    /// <summary>
    /// Convert aggregate method name
    /// </summary>
    private static string TransformAggregateMethodName(string methodName)
    {
        return methodName switch
        {
            "LatestByOffset" => "LATEST_BY_OFFSET",
            "EarliestByOffset" => "EARLIEST_BY_OFFSET",
            "CollectList" => "COLLECT_LIST",
            "CollectSet" => "COLLECT_SET",
            "Average" => "AVG",
            "CountDistinct" => "COUNT_DISTINCT",
            _ => methodName.ToUpper()
        };
    }

    /// <summary>
    /// Handle general expressions
    /// </summary>
    private string ProcessExpression(Expression expression)
    {
        return expression switch
        {
            MethodCallExpression methodCall when KsqlFunctionRegistry.IsAggregateFunction(methodCall.Method.Name)
                => ProcessAggregateFunction(methodCall),
            MethodCallExpression methodCall => KsqlFunctionTranslator.TranslateMethodCall(methodCall),
            MemberExpression member => member.Member.Name,
            ConstantExpression constant => SafeToString(constant.Value),
            BinaryExpression binary => ProcessBinaryExpression(binary),
            UnaryExpression unary => ProcessUnaryExpression(unary),
            _ => expression.ToString()
        };
    }

    /// <summary>
    /// Handle binary expressions
    /// </summary>
    private string ProcessBinaryExpression(BinaryExpression binary)
    {
        var left = ProcessExpression(binary.Left);
        var right = ProcessExpression(binary.Right);
        var varoperator = GetSqlOperator(binary.NodeType);
        return $"({left} {varoperator} {right})";
    }

    /// <summary>
    /// Handle unary expressions
    /// </summary>
    private string ProcessUnaryExpression(UnaryExpression unary)
    {
        return unary.NodeType switch
        {
            ExpressionType.Not => $"NOT ({ProcessExpression(unary.Operand)})",
            ExpressionType.Convert or ExpressionType.ConvertChecked => ProcessExpression(unary.Operand),
            _ => ProcessExpression(unary.Operand)
        };
    }

    /// <summary>
    /// Convert SQL operators
    /// </summary>
    private static string GetSqlOperator(ExpressionType nodeType)
    {
        return nodeType switch
        {
            ExpressionType.Equal => "=",
            ExpressionType.NotEqual => "<>",
            ExpressionType.GreaterThan => ">",
            ExpressionType.GreaterThanOrEqual => ">=",
            ExpressionType.LessThan => "<",
            ExpressionType.LessThanOrEqual => "<=",
            ExpressionType.AndAlso => "AND",
            ExpressionType.OrElse => "OR",
            ExpressionType.Add => "+",
            ExpressionType.Subtract => "-",
            ExpressionType.Multiply => "*",
            ExpressionType.Divide => "/",
            ExpressionType.Modulo => "%",
            _ => throw new NotSupportedException($"Operator {nodeType} is not supported in HAVING clause")
        };
    }

    /// <summary>
    /// NULL-safe string conversion
    /// </summary>
    private static string SafeToString(object? value)
    {
        return BuilderValidation.SafeToString(value);
    }
}
