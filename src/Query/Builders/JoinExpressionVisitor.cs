using Kafka.Ksql.Linq.Query.Builders.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Kafka.Ksql.Linq.Query.Builders;
internal class JoinExpressionVisitor : ExpressionVisitor
{
    private readonly List<JoinInfo> _joins = new();
    private string _result = string.Empty;

    public string GetResult()
    {
        return _result;
    }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        if (node.Method.Name == "Join")
        {
            try
            {
                var joinInfo = ProcessJoinCall(node);
                _joins.Add(joinInfo);

                // Check two-table limit
                if (_joins.Count > JoinLimitationEnforcer.MaxJoinTables - 1) // -1 because base table counts as 1
                {
                    throw new InvalidOperationException(
                        $"Maximum {JoinLimitationEnforcer.MaxJoinTables} tables allowed in JOIN operations");
                }

                _result = BuildJoinQuery(joinInfo);
            }
            catch (Exception ex)
            {
                _result = $"/* JOIN build error: {ex.Message} */";
            }
        }

        return base.VisitMethodCall(node);
    }

    /// <summary>
    /// Handle JOIN invocation
    /// </summary>
    private JoinInfo ProcessJoinCall(MethodCallExpression joinCall)
    {
        if (joinCall.Arguments.Count < 4)
        {
            throw new InvalidOperationException("JOIN requires at least 4 arguments");
        }

        var outerKeySelector = ExtractLambdaExpression(joinCall.Arguments[2]);
        var innerKeySelector = ExtractLambdaExpression(joinCall.Arguments[3]);
        var resultSelector = joinCall.Arguments.Count > 4 ? ExtractLambdaExpression(joinCall.Arguments[4]) : null;

        if (outerKeySelector == null || innerKeySelector == null)
        {
            throw new InvalidOperationException("Unable to extract key selectors from JOIN");
        }

        var outerKeys = ExtractJoinKeys(outerKeySelector.Body);
        var innerKeys = ExtractJoinKeys(innerKeySelector.Body);
        var projections = resultSelector != null ? ExtractProjection(resultSelector.Body) : new List<string>();

        if (outerKeys.Count != innerKeys.Count || outerKeys.Count == 0)
        {
            throw new InvalidOperationException("JOIN keys mismatch or empty");
        }

        // Extract type information
        var outerType = ExtractTypeFromArgument(joinCall.Arguments[0]);
        var innerType = ExtractTypeFromArgument(joinCall.Arguments[1]);

        // Validate JOIN constraints
        JoinLimitationEnforcer.ValidateJoinConstraints(outerType, innerType, outerKeySelector, innerKeySelector);

        return new JoinInfo
        {
            OuterType = outerType.Name,
            InnerType = innerType.Name,
            OuterKeys = outerKeys,
            InnerKeys = innerKeys,
            Projections = projections,
            OuterAlias = outerKeySelector.Parameters.FirstOrDefault()?.Name ?? "o",
            InnerAlias = innerKeySelector.Parameters.FirstOrDefault()?.Name ?? "i"
        };
    }

    /// <summary>
    /// Build JOIN query
    /// </summary>
    private string BuildJoinQuery(JoinInfo joinInfo)
    {
        // Build JOIN condition
        var conditions = new List<string>();
        for (int i = 0; i < joinInfo.OuterKeys.Count; i++)
        {
            conditions.Add($"{joinInfo.OuterAlias}.{joinInfo.OuterKeys[i]} = {joinInfo.InnerAlias}.{joinInfo.InnerKeys[i]}");
        }

        var joinCondition = string.Join(" AND ", conditions);

        // Build projection
        string selectClause;
        if (joinInfo.Projections.Count > 0)
        {
            selectClause = string.Join(", ", joinInfo.Projections);
        }
        else
        {
            // Default: all columns from both tables
            selectClause = $"{joinInfo.OuterAlias}.*, {joinInfo.InnerAlias}.*";
        }

        // Complete JOIN query
        return $"SELECT {selectClause} FROM {joinInfo.OuterType} {joinInfo.OuterAlias} " +
               $"JOIN {joinInfo.InnerType} {joinInfo.InnerAlias} ON {joinCondition}";
    }

    /// <summary>
    /// Extract JOIN keys
    /// </summary>
    private List<string> ExtractJoinKeys(Expression? expr)
    {
        var keys = new List<string>();

        if (expr == null)
            return keys;

        switch (expr)
        {
            case NewExpression newExpr:
                // Composite key (anonymous type)
                foreach (var arg in newExpr.Arguments)
                {
                    var member = ExtractMemberExpression(arg);
                    if (member != null)
                    {
                        keys.Add(member.Member.Name);
                    }
                }
                break;

            case MemberExpression memberExpr:
                // Single key
                keys.Add(memberExpr.Member.Name);
                break;

            case UnaryExpression unaryExpr:
                // Skip type conversions, etc.
                return ExtractJoinKeys(unaryExpr.Operand);
        }

        return keys;
    }

    /// <summary>
    /// Extract projection
    /// </summary>
    private List<string> ExtractProjection(Expression? expr)
    {
        var projections = new List<string>();

        if (expr == null)
            return projections;

        if (expr is NewExpression newExpr)
        {
            for (int i = 0; i < newExpr.Arguments.Count; i++)
            {
                var arg = newExpr.Arguments[i];
                var memberName = newExpr.Members?[i]?.Name ?? $"col{i}";

                if (arg is MemberExpression memberExpr)
                {
                    var tablePrefixProperty = ExtractTablePrefix(memberExpr);
                    if (!string.IsNullOrEmpty(tablePrefixProperty))
                    {
                        projections.Add($"{tablePrefixProperty} AS {memberName}");
                    }
                    else
                    {
                        projections.Add($"{memberExpr.Member.Name} AS {memberName}");
                    }
                }
                else
                {
                    // Use as-is for complex expressions
                    projections.Add($"{ProcessComplexExpression(arg)} AS {memberName}");
                }
            }
        }

        return projections;
    }

    /// <summary>
    /// Extract table prefix
    /// </summary>
    private string ExtractTablePrefix(MemberExpression memberExpr)
    {
        if (memberExpr.Expression is ParameterExpression pe)
        {
            return $"{pe.Name}.{memberExpr.Member.Name}";
        }

        if (memberExpr.Expression is MemberExpression me && me.Expression is ParameterExpression mpe)
        {
            return $"{mpe.Name}.{memberExpr.Member.Name}";
        }

        return string.Empty;
    }

    /// <summary>
    /// Handle complex expressions
    /// </summary>
    private string ProcessComplexExpression(Expression expr)
    {
        return expr switch
        {
            MemberExpression member => member.Member.Name,
            ConstantExpression constant => BuilderValidation.SafeToString(constant.Value),
            BinaryExpression binary => $"({ProcessComplexExpression(binary.Left)} {GetOperator(binary.NodeType)} {ProcessComplexExpression(binary.Right)})",
            _ => expr.ToString()
        };
    }

    /// <summary>
    /// Convert operators
    /// </summary>
    private static string GetOperator(ExpressionType nodeType)
    {
        return nodeType switch
        {
            ExpressionType.Equal => "=",
            ExpressionType.NotEqual => "!=",
            ExpressionType.GreaterThan => ">",
            ExpressionType.GreaterThanOrEqual => ">=",
            ExpressionType.LessThan => "<",
            ExpressionType.LessThanOrEqual => "<=",
            _ => throw new NotSupportedException($"Operator {nodeType} is not supported in JOIN clause")
        };
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
    /// Extract MemberExpression
    /// </summary>
    private static MemberExpression? ExtractMemberExpression(Expression expr)
    {
        return expr switch
        {
            MemberExpression m => m,
            UnaryExpression u when u.Operand is MemberExpression m => m,
            _ => null
        };
    }

    /// <summary>
    /// Extract type from argument
    /// </summary>
    private static Type ExtractTypeFromArgument(Expression arg)
    {
        // Extract T from IEnumerable<T>
        var type = arg.Type;
        if (type.IsGenericType)
        {
            var genericArgs = type.GetGenericArguments();
            if (genericArgs.Length > 0)
            {
                return genericArgs[0];
            }
        }

        return typeof(object);
    }
}
