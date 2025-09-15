using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Kafka.Ksql.Linq.Query.Builders;
internal class OrderByExpressionVisitor : ExpressionVisitor
{
    private readonly List<string> _orderClauses = new();

    public string GetResult()
    {
        return _orderClauses.Count > 0 ? string.Join(", ", _orderClauses) : string.Empty;
    }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        var methodName = node.Method.Name;

        switch (methodName)
        {
            case "OrderBy":
                ProcessOrderByCall(node, "ASC");
                break;

            case "OrderByDescending":
                ProcessOrderByCall(node, "DESC");
                break;

            case "ThenBy":
                ProcessThenByCall(node, "ASC");
                break;

            case "ThenByDescending":
                ProcessThenByCall(node, "DESC");
                break;

            default:
                // Process other method calls recursively
                base.VisitMethodCall(node);
                break;
        }

        return node;
    }

    /// <summary>
    /// Handle OrderBy/OrderByDescending
    /// </summary>
    private void ProcessOrderByCall(MethodCallExpression node, string direction)
    {
        // Process previous method chain first
        if (node.Object != null)
        {
            Visit(node.Object);
        }

        // Process the current OrderBy
        if (node.Arguments.Count >= 2)
        {
            var keySelector = ExtractLambdaExpression(node.Arguments[1]);
            if (keySelector != null)
            {
                var columnName = ExtractColumnName(keySelector.Body);
                _orderClauses.Add($"{columnName} {direction}");
            }
        }
    }

    /// <summary>
    /// Handle ThenBy/ThenByDescending
    /// </summary>
    private void ProcessThenByCall(MethodCallExpression node, string direction)
    {
        // Process previous method chain first
        if (node.Object != null)
        {
            Visit(node.Object);
        }

        // Process the current ThenBy
        if (node.Arguments.Count >= 2)
        {
            var keySelector = ExtractLambdaExpression(node.Arguments[1]);
            if (keySelector != null)
            {
                var columnName = ExtractColumnName(keySelector.Body);
                _orderClauses.Add($"{columnName} {direction}");
            }
        }
    }

    /// <summary>
    /// Extract lambda expression
    /// </summary>
    private static LambdaExpression? ExtractLambdaExpression(Expression expr)
    {
        return expr switch
        {
            UnaryExpression unary when unary.Operand is LambdaExpression lambda => lambda,
            LambdaExpression lambda => lambda,
            _ => null
        };
    }

    /// <summary>
    /// Extract column name
    /// </summary>
    private string ExtractColumnName(Expression expr)
    {
        return expr switch
        {
            MemberExpression member => GetMemberName(member),
            UnaryExpression unary when unary.NodeType == ExpressionType.Convert => ExtractColumnName(unary.Operand),
            MethodCallExpression method => ProcessOrderByFunction(method),
            _ => throw new InvalidOperationException($"Unsupported ORDER BY expression: {expr.GetType().Name}")
        };
    }

    /// <summary>
    /// Get member name
    /// </summary>
    private static string GetMemberName(MemberExpression member)
    {
        // Use the deepest property name for nested properties
        return member.Member.Name;
    }

    /// <summary>
    /// Handle ORDER BY functions
    /// </summary>
    private string ProcessOrderByFunction(MethodCallExpression methodCall)
    {
        var methodName = methodCall.Method.Name;

        // Only a limited set of functions is allowed in ORDER BY
        return methodName switch
        {
            // Window functions
            "RowTime" => "ROWTIME",

            // String functions (partial)
            "ToUpper" => ProcessSimpleFunction("UPPER", methodCall),
            "ToLower" => ProcessSimpleFunction("LOWER", methodCall),

            // Numeric functions (partial)
            "Abs" => ProcessSimpleFunction("ABS", methodCall),

            // Date functions (partial)
            "Year" => ProcessSimpleFunction("YEAR", methodCall),
            "Month" => ProcessSimpleFunction("MONTH", methodCall),
            "Day" => ProcessSimpleFunction("DAY", methodCall),

            _ => throw new InvalidOperationException($"Function '{methodName}' is not supported in ORDER BY clause")
        };
    }

    /// <summary>
    /// Handle simple functions
    /// </summary>
    private string ProcessSimpleFunction(string ksqlFunction, MethodCallExpression methodCall)
    {
        var target = methodCall.Object ?? methodCall.Arguments[0];
        var columnName = ExtractColumnName(target);
        return $"{ksqlFunction}({columnName})";
    }
}

/// <summary>
