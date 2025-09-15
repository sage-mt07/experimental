using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Query.Builders.Common;
using Kafka.Ksql.Linq.Query.Builders.Functions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace Kafka.Ksql.Linq.Query.Builders;
/// <summary>
/// ExpressionVisitor specialized for SELECT clause
/// </summary>
internal class SelectExpressionVisitor : ExpressionVisitor
{
    private readonly List<string> _columns = new();
    private readonly HashSet<string> _usedAliases = new();
    private readonly HashSet<string> _selectedGroupKeys = new();
    private readonly System.Collections.Generic.IDictionary<string, string>? _paramToSource;
    private readonly System.Collections.Generic.ISet<string>? _excludeAliases;
    private bool _sawAggregate;
    private bool _sawNonAggregate;
    private bool _inGroupingContext;

    public SelectExpressionVisitor() { }
    public SelectExpressionVisitor(System.Collections.Generic.IDictionary<string, string> paramToSource)
    {
        _paramToSource = paramToSource;
    }
    public SelectExpressionVisitor(System.Collections.Generic.IDictionary<string, string> paramToSource, System.Collections.Generic.ISet<string> excludeAliases)
    {
        _paramToSource = paramToSource;
        _excludeAliases = new HashSet<string>(excludeAliases);
    }

    public string GetResult()
    {
        return _columns.Count > 0 ? string.Join(", ", _columns) : string.Empty;
    }

    protected override Expression VisitNew(NewExpression node)
    {
        // Detect grouping context upfront to avoid false-positive mix checks
        _inGroupingContext = _inGroupingContext || ContainsGroupingParameter(node);
        // When used with object initializers, the constructor is parameterless
        // and does not map DTO members. In such cases validation should occur
        // in VisitMemberInit instead of here to avoid false mismatches.
        if (node.Members is { Count: > 0 } || node.Arguments.Count > 0)
        {
            ValidateGroupKeyDtoOrder(node);
        }
        // Handle anonymous type projections
        for (int i = 0; i < node.Arguments.Count; i++)
        {
            var arg = node.Arguments[i];
            var memberName = node.Members?[i]?.Name ?? $"col{i}";

            if (IsGroupKeyObject(arg))
            {
                AddGroupKeyColumns();
            }
            else
            {
                EnforceNoMixAggregateWithoutGroupBy(arg);
                var columnExpression = ProcessProjectionArgument(arg);
                var alias = GenerateUniqueAlias(memberName);
                if (!string.Equals(columnExpression, alias, StringComparison.OrdinalIgnoreCase))
                    _columns.Add($"{columnExpression} AS {alias}");
                else
                    _columns.Add(columnExpression);
            }
        }

        return node;
    }

    protected override Expression VisitMemberInit(MemberInitExpression node)
    {
        // Detect grouping context upfront to avoid false-positive mix checks
        _inGroupingContext = _inGroupingContext || ContainsGroupingParameter(node);
        ValidateGroupKeyDtoOrder(node);
        foreach (var binding in node.Bindings.OfType<MemberAssignment>())
        {
            var memberName = binding.Member.Name;
            var expr = binding.Expression;
            if (IsGroupKeyObject(expr))
            {
                AddGroupKeyColumns();
            }
            else
            {
                EnforceNoMixAggregateWithoutGroupBy(expr);
                var columnExpression = ProcessProjectionArgument(expr);
                var alias = GenerateUniqueAlias(memberName);
                var needsAlias = !string.Equals(columnExpression, alias, StringComparison.OrdinalIgnoreCase)
                                || IsGroupKeyMember(expr);
                if (needsAlias)
                    _columns.Add($"{columnExpression} AS {alias}");
                else
                    _columns.Add(columnExpression);
            }
        }
        return node;
    }

    protected override Expression VisitMember(MemberExpression node)
    {
        // Track grouping context when rooted at IGrouping parameter
        if (!_inGroupingContext && GetRootParameter(node) is ParameterExpression pe && IsIGroupingType(pe.Type))
        {
            _inGroupingContext = true;
        }
        if (IsGroupKeyObject(node))
        {
            AddGroupKeyColumns();
        }
        else
        {
            // Simple property access
            var columnName = GetColumnName(node);
            _columns.Add(columnName);
        }
        return node;
    }

    protected override Expression VisitParameter(ParameterExpression node)
    {
        // For SELECT *
        _columns.Add("*");
        return node;
    }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        // Handle function calls
        var functionCall = KsqlFunctionTranslator.TranslateMethodCall(node);
        _columns.Add(functionCall);
        return node;
    }

    protected override Expression VisitBinary(BinaryExpression node)
    {
        // Handle arithmetic expressions
        var left = ProcessExpression(node.Left);
        var right = ProcessExpression(node.Right);
        var varoperator = GetOperator(node.NodeType);

        var expression = $"({left} {varoperator} {right})";
        _columns.Add(expression);
        return node;
    }

    protected override Expression VisitConditional(ConditionalExpression node)
    {
        // Handle CASE expressions
        BuilderValidation.ValidateConditionalTypes(node.IfTrue, node.IfFalse);
        var test = ProcessExpression(node.Test);
        var ifTrue = ProcessExpression(node.IfTrue);
        var ifFalse = ProcessExpression(node.IfFalse);

        var caseExpression = $"CASE WHEN {test} THEN {ifTrue} ELSE {ifFalse} END";
        _columns.Add(caseExpression);
        return node;
    }

    /// <summary>
    /// Process projection argument
    /// </summary>
    private string ProcessProjectionArgument(Expression arg)
    {
        return arg switch
        {
            MemberExpression member => GetColumnName(member),
            MethodCallExpression methodCall => KsqlFunctionTranslator.TranslateMethodCall(methodCall),
            BinaryExpression binary => ProcessBinaryExpression(binary),
            ConstantExpression constant => SafeToString(constant.Value),
            ConditionalExpression conditional => ProcessConditionalExpression(conditional),
            UnaryExpression unary => ProcessExpression(unary.Operand),
            _ => ProcessExpression(arg)
        };
    }

    private static bool IsAggregateExpression(Expression expr)
    {
        if (expr is MethodCallExpression mc)
        {
            if (KsqlFunctionRegistry.IsAggregateFunction(mc.Method.Name)) return true;
            // Inspect inner arguments in case aggregates are nested inside
            foreach (var a in mc.Arguments)
                if (IsAggregateExpression(a)) return true;
            if (mc.Object != null && IsAggregateExpression(mc.Object)) return true;
        }
        else if (expr is UnaryExpression ue)
        {
            return IsAggregateExpression(ue.Operand);
        }
        else if (expr is BinaryExpression be)
        {
            return IsAggregateExpression(be.Left) || IsAggregateExpression(be.Right);
        }
        else if (expr is ConditionalExpression ce)
        {
            return IsAggregateExpression(ce.Test) || IsAggregateExpression(ce.IfTrue) || IsAggregateExpression(ce.IfFalse);
        }
        else if (expr is MemberInitExpression mi)
        {
            return mi.Bindings.OfType<MemberAssignment>().Any(b => IsAggregateExpression(b.Expression));
        }
        else if (expr is NewExpression ne)
        {
            return ne.Arguments.Any(IsAggregateExpression);
        }
        return false;
    }

    private void EnforceNoMixAggregateWithoutGroupBy(Expression expr)
    {
        // Allow mixing aggregate and non-aggregate when projecting from a grouping context
        if (_inGroupingContext)
            return;
        var isAgg = IsAggregateExpression(expr);
        if (isAgg)
        {
            if (_sawNonAggregate)
                throw new InvalidOperationException("SELECT clause cannot mix aggregate functions with non-aggregate columns without GROUP BY");
            _sawAggregate = true;
        }
        else
        {
            if (_sawAggregate)
                throw new InvalidOperationException("SELECT clause cannot mix aggregate functions with non-aggregate columns without GROUP BY");
            _sawNonAggregate = true;
        }
    }

    private static bool IsIGroupingType(Type t)
        => t.IsGenericType && t.GetGenericTypeDefinition() == typeof(IGrouping<,>);

    private static ParameterExpression? GetRootParameter(MemberExpression me)
    {
        Expression? e = me.Expression;
        while (e is MemberExpression m)
            e = m.Expression;
        return e as ParameterExpression;
    }

    private static bool ContainsGroupingParameter(Expression expr)
    {
        var found = false;
        void Walk(Expression? e)
        {
            if (e == null || found) return;
            switch (e)
            {
                case ParameterExpression pe:
                    if (IsIGroupingType(pe.Type)) { found = true; return; }
                    break;
                case MemberExpression me:
                    Walk(me.Expression);
                    break;
                case MethodCallExpression mc:
                    if (mc.Object != null) Walk(mc.Object);
                    foreach (var a in mc.Arguments) Walk(a);
                    break;
                case UnaryExpression ue:
                    Walk(ue.Operand);
                    break;
                case BinaryExpression be:
                    Walk(be.Left); Walk(be.Right);
                    break;
                case ConditionalExpression ce:
                    Walk(ce.Test); Walk(ce.IfTrue); Walk(ce.IfFalse);
                    break;
                case NewExpression ne:
                    foreach (var a in ne.Arguments) Walk(a);
                    break;
                case MemberInitExpression mi:
                    Walk(mi.NewExpression);
                    foreach (var b in mi.Bindings)
                        if (b is MemberAssignment ma) Walk(ma.Expression);
                    break;
            }
        }
        Walk(expr);
        return found;
    }

    /// <summary>
    /// Process binary expression
    /// </summary>
    private string ProcessBinaryExpression(BinaryExpression binary)
    {
        var left = ProcessExpression(binary.Left);
        var right = ProcessExpression(binary.Right);
        var varoperator = GetOperator(binary.NodeType);
        return $"({left} {varoperator} {right})";
    }

    /// <summary>
    /// Process conditional expression
    /// </summary>
    private string ProcessConditionalExpression(ConditionalExpression conditional)
    {
        var test = ProcessExpression(conditional.Test);
        var ifTrue = ProcessExpression(conditional.IfTrue);
        var ifFalse = ProcessExpression(conditional.IfFalse);
        return $"CASE WHEN {test} THEN {ifTrue} ELSE {ifFalse} END";
    }

    /// <summary>
    /// Process general expression
    /// </summary>
    private string ProcessExpression(Expression expression)
    {
        return expression switch
        {
            MemberExpression member => GetColumnName(member),
            ConstantExpression constant => SafeToString(constant.Value),
            MethodCallExpression methodCall => KsqlFunctionTranslator.TranslateMethodCall(methodCall),
            BinaryExpression binary => ProcessBinaryExpression(binary),
            UnaryExpression unary => ProcessExpression(unary.Operand),
            _ => expression.ToString()
        };
    }

    /// <summary>
    /// Get column name
    /// </summary>
    private string GetColumnName(MemberExpression member)
    {
        // Break down property access chain
        var stack = new System.Collections.Generic.Stack<string>();
        Expression? expr = member;
        while (expr is MemberExpression me)
        {
            stack.Push(me.Member.Name);
            expr = me.Expression;
        }

        if (expr is not ParameterExpression pe)
            throw new InvalidOperationException("Unqualified column access is not allowed. Use source parameter properties.");

        var path = stack.ToArray();

        // Handle g.Key.X pattern
        if (path.Length > 0 && path[0] == "Key")
        {
            var prefix = GetKeyPrefix(pe);
            var cols = path[1..];
            for (int i = 0; i < cols.Length; i++)
                cols[i] = KsqlNameUtils.Sanitize(cols[i]).ToUpperInvariant();
            var name = string.Join('.', cols);
            return $"{prefix}.{name}";
        }

        if (member.Member is PropertyInfo prop && prop.GetCustomAttribute<KsqlKeyAttribute>() != null)
        {
            var prefix = GetKeyPrefix(pe);
            var name = KsqlNameUtils.Sanitize(prop.Name).ToUpperInvariant();
            return $"{prefix}.{name}";
        }

        // Regular property access
        for (int i = 0; i < path.Length; i++)
            path[i] = KsqlNameUtils.Sanitize(path[i]);

        var col = string.Join(".", path);
        if (_paramToSource != null && _paramToSource.TryGetValue(pe.Name ?? string.Empty, out var source))
            return $"{source}.{col}";
        return col;
    }

    /// <summary>
    /// Generate unique alias
    /// </summary>
    private string GenerateUniqueAlias(string baseName)
    {
        var alias = baseName;
        var counter = 1;

        while (_usedAliases.Contains(alias))
        {
            alias = $"{baseName}_{counter}";
            counter++;
        }

        _usedAliases.Add(alias);
        return alias;
    }

    /// <summary>
    /// Convert operators
    /// </summary>
    private static string GetOperator(ExpressionType nodeType)
    {
        return nodeType switch
        {
            ExpressionType.Add => "+",
            ExpressionType.Subtract => "-",
            ExpressionType.Multiply => "*",
            ExpressionType.Divide => "/",
            ExpressionType.Modulo => "%",
            ExpressionType.Equal => "=",
            ExpressionType.NotEqual => "<>",
            ExpressionType.GreaterThan => ">",
            ExpressionType.GreaterThanOrEqual => ">=",
            ExpressionType.LessThan => "<",
            ExpressionType.LessThanOrEqual => "<=",
            ExpressionType.AndAlso => "AND",
            ExpressionType.OrElse => "OR",
            _ => throw new NotSupportedException($"Operator {nodeType} is not supported in SELECT clause")
        };
    }

    /// <summary>
    /// NULL-safe string conversion
    /// </summary>
    private static string SafeToString(object? value)
    {
        return BuilderValidation.SafeToString(value);
    }

    private static bool IsGroupKeyObject(Expression expr)
    {
        return expr is MemberExpression member &&
               member.Member.Name == "Key" &&
               member.Expression is ParameterExpression;
    }

    private static bool IsGroupKeyMember(Expression expr)
    {
        if (expr is not MemberExpression member)
            return false;

        if (member.Member.Name == "Key" && member.Expression is ParameterExpression)
            return true;

        if (member.Expression is MemberExpression inner &&
            inner.Member.Name == "Key" &&
            inner.Expression is ParameterExpression)
            return true;

        return false;
    }

    private IEnumerable<(string expr, string alias)> GetGroupKeyColumns()
    {
        var keys = GroupByClauseBuilder.LastBuiltKeys;
        if (string.IsNullOrWhiteSpace(keys))
            return Array.Empty<(string, string)>();

        return keys
            .Split(',')
            .Select(s => s.Trim())
            .Where(s => s.Length > 0)
            .Select(s =>
            {
                // Derive a safe alias from the last identifier token in the expression
                var m = System.Text.RegularExpressions.Regex.Match(s, @"([A-Za-z_][A-Za-z0-9_]*)\)?$");
                var alias = m.Success ? m.Groups[1].Value : s;
                return (s, alias);
            });
    }

    private void AddGroupKeyColumns()
    {
        foreach (var (expr, alias) in GetGroupKeyColumns())
        {
            if (_selectedGroupKeys.Contains(alias))
                continue;
            _selectedGroupKeys.Add(alias);
            _columns.Add($"{expr} AS {alias}");
        }
    }

    private string GetKeyPrefix(ParameterExpression pe)
    {
        if (_paramToSource != null && _paramToSource.TryGetValue(pe.Name ?? string.Empty, out var alias))
            return alias;

        var type = GetGroupingElementType(pe.Type);
        return KeyNameResolver.GetKeyPrefix(type).Replace(".key", string.Empty);
    }

    private static Type GetGroupingElementType(Type type)
    {
        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IGrouping<,>))
            return type.GetGenericArguments()[1];
        return type;
    }

    private void ValidateGroupKeyDtoOrder(NewExpression node)
    {
        if (node.Type == null)
            return;

        if (IsAnonymousType(node.Type))
            return;

        var groupKeys = GetGroupKeyColumns().Select(x => x.alias).ToList();
        if (groupKeys.Count == 0)
            return;

        var dtoKeyMembers = node.Arguments
            .Zip(node.Members ?? Enumerable.Empty<MemberInfo>(), (arg, mem) => new { arg, mem })
            .Where(x => IsGroupKeyMember(x.arg))
            .Select(x => x.mem.Name)
            .ToList();

        if (dtoKeyMembers.Count != groupKeys.Count || !dtoKeyMembers.SequenceEqual(groupKeys, StringComparer.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                "The order of GroupBy keys does not match the output DTO definition. Please ensure they are the same order.");
        }
    }

    private void ValidateGroupKeyDtoOrder(MemberInitExpression node)
    {
        if (IsAnonymousType(node.Type))
            return;

        var groupKeys = GetGroupKeyColumns().Select(x => x.alias).ToList();
        if (groupKeys.Count == 0)
            return;

        var dtoKeyMembers = node.Bindings
            .OfType<MemberAssignment>()
            .Where(b => IsGroupKeyMember(b.Expression))
            .Select(b => b.Member.Name)
            .ToList();

        if (dtoKeyMembers.Count != groupKeys.Count || !dtoKeyMembers.SequenceEqual(groupKeys, StringComparer.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                "The order of GroupBy keys does not match the output DTO definition. Please ensure they are the same order.");
        }
    }

    private static bool IsAnonymousType(Type type)
    {
        return Attribute.IsDefined(type, typeof(CompilerGeneratedAttribute)) &&
               type.IsGenericType && type.Name.Contains("AnonymousType");
    }
}
