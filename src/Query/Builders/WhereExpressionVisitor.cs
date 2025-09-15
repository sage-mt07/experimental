using Kafka.Ksql.Linq.Query.Builders.Common;
using Kafka.Ksql.Linq.Query.Builders.Functions;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

/// <summary>
/// ExpressionVisitor specialized for WHERE clause
/// </summary>
internal class WhereExpressionVisitor : ExpressionVisitor
{
    private readonly Stack<string> _conditionStack = new();
    private string _result = string.Empty;
    private readonly System.Collections.Generic.IDictionary<string, string>? _paramToAlias;

    public WhereExpressionVisitor() { }
    public WhereExpressionVisitor(System.Collections.Generic.IDictionary<string, string> paramToAlias)
    {
        _paramToAlias = paramToAlias;
    }

    public string GetResult()
    {
        return _result;
    }

    protected override Expression VisitBinary(BinaryExpression node)
    {
        // Special handling for NULL comparisons
        if (IsNullComparison(node))
        {
            _result = HandleNullComparison(node);
            return node;
        }

        // Handle composite key comparisons
        if (IsCompositeKeyComparison(node))
        {
            _result = HandleCompositeKeyComparison(node);
            return node;
        }

        // Handle standard binary operations
        var left = ProcessExpression(node.Left);
        var right = ProcessExpression(node.Right);
        var varoperator = GetSqlOperator(node.NodeType);

        _result = $"({left} {varoperator} {right})";
        return node;
    }

    protected override Expression VisitUnary(UnaryExpression node)
    {
        switch (node.NodeType)
        {
            case ExpressionType.Not:
                _result = HandleNotExpression(node);
                break;

            case ExpressionType.Convert:
            case ExpressionType.ConvertChecked:
                // For type conversions, process the inner expression
                Visit(node.Operand);
                break;

            default:
                Visit(node.Operand);
                break;
        }

        return node;
    }

    protected override Expression VisitMember(MemberExpression node)
    {
        // Handle property access
        _result = HandleMemberAccess(node);
        return node;
    }

    protected override Expression VisitConstant(ConstantExpression node)
    {
        _result = SafeToString(node.Value);
        return node;
    }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        // Handle method calls
        _result = HandleMethodCall(node);
        return node;
    }

    /// <summary>
    /// Detect NULL comparisons
    /// </summary>
    private static bool IsNullComparison(BinaryExpression node)
    {
        return (node.NodeType == ExpressionType.Equal || node.NodeType == ExpressionType.NotEqual) &&
               (IsNullConstant(node.Left) || IsNullConstant(node.Right));
    }

    /// <summary>
    /// Detect NULL constants
    /// </summary>
    private static bool IsNullConstant(Expression expr)
    {
        return expr is ConstantExpression constant && constant.Value == null;
    }

    /// <summary>
    /// Detect composite key comparisons
    /// </summary>
    private static bool IsCompositeKeyComparison(BinaryExpression node)
    {
        return node.NodeType == ExpressionType.Equal &&
               node.Left is NewExpression &&
               node.Right is NewExpression;
    }

    /// <summary>
    /// Handle NULL comparisons
    /// </summary>
    private string HandleNullComparison(BinaryExpression node)
    {
        var memberExpr = IsNullConstant(node.Left) ? node.Right : node.Left;
        var memberName = ProcessExpression(memberExpr);
        var isNotEqual = node.NodeType == ExpressionType.NotEqual;

        return $"{memberName} IS {(isNotEqual ? "NOT " : "")}NULL";
    }

    /// <summary>
    /// Handle composite key comparisons
    /// </summary>
    private string HandleCompositeKeyComparison(BinaryExpression node)
    {
        var leftNew = (NewExpression)node.Left;
        var rightNew = (NewExpression)node.Right;

        if (leftNew.Arguments.Count != rightNew.Arguments.Count)
        {
            throw new InvalidOperationException("Composite key expressions must have the same number of properties");
        }

        var conditions = new List<string>();

        for (int i = 0; i < leftNew.Arguments.Count; i++)
        {
            var leftMember = ProcessExpression(leftNew.Arguments[i]);
            var rightMember = ProcessExpression(rightNew.Arguments[i]);
            conditions.Add($"{leftMember} = {rightMember}");
        }

        return conditions.Count == 1 ? conditions[0] : $"({string.Join(" AND ", conditions)})";
    }

    /// <summary>
    /// Handle NOT expressions
    /// </summary>
    private string HandleNotExpression(UnaryExpression node)
    {
        // Handle Nullable<bool>.Value access
        if (node.Operand is MemberExpression member &&
            member.Member.Name == "Value" &&
            member.Expression is MemberExpression innerMember &&
            innerMember.Type == typeof(bool?))
        {
            var memberName = GetMemberName(innerMember);
            return $"({memberName} = false)";
        }

        // Regular boolean negation
        if (node.Operand is MemberExpression regularMember &&
            (regularMember.Type == typeof(bool) || regularMember.Type == typeof(bool?)))
        {
            var memberName = GetMemberName(regularMember);
            return $"({memberName} = false)";
        }

        // Negation of IEnumerable.Contains
        if (node.Operand is MethodCallExpression method && IsEnumerableContains(method))
        {
            return BuildInExpression(method, negated: true);
        }

        // Negation of complex expressions
        var operand = ProcessExpression(node.Operand);
        return $"NOT ({operand})";
    }

    /// <summary>
    /// Handle member access
    /// </summary>
    private string HandleMemberAccess(MemberExpression node)
    {
        // Accessing Nullable<bool>.Value
        if (node.Member.Name == "Value" &&
            node.Expression is MemberExpression innerMember &&
            innerMember.Type == typeof(bool?))
        {
            var memberName = GetMemberName(innerMember);
            return $"({memberName} = true)";
        }

        // Accessing HasValue property
        if (node.Member.Name == "HasValue" &&
            node.Expression != null &&
            Nullable.GetUnderlyingType(node.Expression.Type) != null)
        {
            var memberName = GetMemberName((MemberExpression)node.Expression);
            return $"{memberName} IS NOT NULL";
        }

        // Regular property access
        var finalMemberName = GetMemberName(node);

        // Explicitly compare bool properties with = true
        if (node.Type == typeof(bool) || node.Type == typeof(bool?))
        {
            return $"({finalMemberName} = true)";
        }

        return finalMemberName;
    }

    /// <summary>
    /// Handle method calls
    /// </summary>
    private string HandleMethodCall(MethodCallExpression node)
    {
        var methodName = node.Method.Name;

        // Special handling for string methods
        switch (methodName)
        {
            case "Contains":
                return HandleContainsMethod(node);
            case "StartsWith":
                return HandleStartsWithMethod(node);
            case "EndsWith":
                return HandleEndsWithMethod(node);
            default:
                // General function translation
                return KsqlFunctionTranslator.TranslateMethodCall(node);
        }
    }

    /// <summary>
    /// Handle Contains method
    /// </summary>
    private string HandleContainsMethod(MethodCallExpression node)
    {
        // string.Contains pattern
        if (node.Object != null && node.Arguments.Count == 1)
        {
            var target = ProcessExpression(node.Object);
            var value = ProcessExpression(node.Arguments[0]);
            return $"INSTR({target}, {value}) > 0";
        }

        // IEnumerable.Contains pattern
        if (IsEnumerableContains(node))
        {
            return BuildInExpression(node, negated: false);
        }

        return KsqlFunctionTranslator.TranslateMethodCall(node);
    }

    /// <summary>
    /// Handle StartsWith method
    /// </summary>
    private string HandleStartsWithMethod(MethodCallExpression node)
    {
        if (node.Object != null && node.Arguments.Count == 1)
        {
            var target = ProcessExpression(node.Object);
            var value = ProcessExpression(node.Arguments[0]);
            return $"STARTS_WITH({target}, {value})";
        }

        return KsqlFunctionTranslator.TranslateMethodCall(node);
    }

    /// <summary>
    /// Handle EndsWith method
    /// </summary>
    private string HandleEndsWithMethod(MethodCallExpression node)
    {
        if (node.Object != null && node.Arguments.Count == 1)
        {
            var target = ProcessExpression(node.Object);
            var value = ProcessExpression(node.Arguments[0]);
            return $"ENDS_WITH({target}, {value})";
        }

        return KsqlFunctionTranslator.TranslateMethodCall(node);
    }

    /// <summary>
    /// Generate IN / NOT IN clause from IEnumerable.Contains
    /// </summary>
    private string BuildInExpression(MethodCallExpression node, bool negated)
    {
        var valuesExpr = node.Object == null ? node.Arguments[0] : node.Object;
        var targetExpr = node.Object == null ? node.Arguments[1] : node.Arguments[0];

        var values = EvaluateEnumerable(valuesExpr);
        if (values == null)
        {
            return KsqlFunctionTranslator.TranslateMethodCall(node);
        }

        var joined = string.Join(", ", values.Cast<object>().Select(SafeToString));
        var target = ProcessExpression(targetExpr);
        var op = negated ? "NOT IN" : "IN";
        return $"{target} {op} ({joined})";
    }

    /// <summary>
    /// Determine IEnumerable.Contains
    /// </summary>
    private static bool IsEnumerableContains(MethodCallExpression node)
    {
        if (node.Method.Name != "Contains")
            return false;

        var enumerableType = typeof(IEnumerable);
        if (node.Object == null && node.Arguments.Count == 2)
        {
            return enumerableType.IsAssignableFrom(node.Arguments[0].Type);
        }

        if (node.Object != null && node.Arguments.Count == 1)
        {
            return enumerableType.IsAssignableFrom(node.Object.Type);
        }

        return false;
    }

    /// <summary>
    /// Evaluate expression to obtain IEnumerable
    /// </summary>
    private static IEnumerable? EvaluateEnumerable(Expression expr)
    {
        if (expr is ConstantExpression constant && constant.Value is IEnumerable en)
        {
            return en;
        }

        try
        {
            var lambda = Expression.Lambda(expr);
            var compiled = lambda.Compile();
            var value = compiled.DynamicInvoke();
            return value as IEnumerable;
        }
        catch
        {
            return null;
        }
    }

    /// <summary>
    /// Handle general expressions
    /// </summary>
    private string ProcessExpression(Expression expression)
    {
        return expression switch
        {
            MemberExpression member => GetMemberName(member),
            ConstantExpression constant => SafeToString(constant.Value),
            MethodCallExpression methodCall => KsqlFunctionTranslator.TranslateMethodCall(methodCall),
            BinaryExpression binary => ProcessBinaryExpression(binary),
            UnaryExpression unary when unary.NodeType == ExpressionType.Convert => ProcessExpression(unary.Operand),
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
    /// Get member name
    /// </summary>
    private string GetMemberName(MemberExpression member)
    {
        // Identify root parameter and apply alias
        System.Linq.Expressions.Expression? e = member;
        while (e is MemberExpression me)
            e = me.Expression;
        if (e is ParameterExpression pe && _paramToAlias != null && _paramToAlias.TryGetValue(pe.Name ?? string.Empty, out var alias))
        {
            return $"{alias}.{member.Member.Name}";
        }
        return member.Member.Name;
    }

    /// <summary>
    /// Convert SQL operators
    /// </summary>
    private static string GetSqlOperator(ExpressionType nodeType)
    {
        return nodeType switch
        {
            ExpressionType.Equal => "=",
            ExpressionType.NotEqual => "!=",
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
            _ => throw new NotSupportedException($"Operator {nodeType} is not supported in WHERE clause")
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
