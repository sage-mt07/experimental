using Kafka.Ksql.Linq.Query.Builders.Functions;
using System;
using System.Linq.Expressions;

namespace Kafka.Ksql.Linq.Query.Builders.Common;

/// <summary>
/// Common builder validation
/// Rationale: provide unified validation logic across all builder classes.
/// </summary>
internal static class BuilderValidation
{
    /// <summary>
    /// Basic validation for expression trees
    /// </summary>
    public static void ValidateExpression(Expression expression)
    {
        if (expression == null)
        {
            throw new ArgumentNullException(nameof(expression), "Expression cannot be null");
        }

        ValidateExpressionDepth(expression, maxDepth: 50);
        ValidateExpressionComplexity(expression);
    }

    /// <summary>
    /// Check expression tree depth (prevent stack overflow)
    /// </summary>
    private static void ValidateExpressionDepth(Expression expression, int maxDepth, int currentDepth = 0)
    {
        if (currentDepth > maxDepth)
        {
            throw new InvalidOperationException($"Expression depth exceeds maximum allowed depth of {maxDepth}. " +
                "Consider simplifying the expression or breaking it into multiple operations.");
        }

        switch (expression)
        {
            case BinaryExpression binary:
                ValidateExpressionDepth(binary.Left, maxDepth, currentDepth + 1);
                ValidateExpressionDepth(binary.Right, maxDepth, currentDepth + 1);
                break;

            case UnaryExpression unary:
                ValidateExpressionDepth(unary.Operand, maxDepth, currentDepth + 1);
                break;

            case MethodCallExpression methodCall:
                if (methodCall.Object != null)
                    ValidateExpressionDepth(methodCall.Object, maxDepth, currentDepth + 1);

                foreach (var arg in methodCall.Arguments)
                    ValidateExpressionDepth(arg, maxDepth, currentDepth + 1);
                break;

            case LambdaExpression lambda:
                ValidateExpressionDepth(lambda.Body, maxDepth, currentDepth + 1);
                break;

            case NewExpression newExpr:
                foreach (var arg in newExpr.Arguments)
                    ValidateExpressionDepth(arg, maxDepth, currentDepth + 1);
                break;

            case ConditionalExpression conditional:
                ValidateExpressionDepth(conditional.Test, maxDepth, currentDepth + 1);
                ValidateExpressionDepth(conditional.IfTrue, maxDepth, currentDepth + 1);
                ValidateExpressionDepth(conditional.IfFalse, maxDepth, currentDepth + 1);
                break;
        }
    }

    /// <summary>
    /// Check expression tree complexity
    /// </summary>
    private static void ValidateExpressionComplexity(Expression expression)
    {
        var nodeCount = CountNodes(expression);
        const int maxNodes = 1000;

        if (nodeCount > maxNodes)
        {
            throw new InvalidOperationException($"Expression complexity exceeds maximum allowed nodes of {maxNodes}. " +
                $"Current expression has {nodeCount} nodes. " +
                "Consider simplifying the expression or breaking it into multiple operations.");
        }
    }

    /// <summary>
    /// Count nodes in expression tree
    /// </summary>
    private static int CountNodes(Expression expression)
    {
        var visitor = new NodeCountVisitor();
        visitor.Visit(expression);
        return visitor.NodeCount;
    }

    /// <summary>
    /// Safely extract Body from a Lambda expression
    /// </summary>
    public static Expression? ExtractLambdaBody(Expression expression)
    {
        return expression switch
        {
            LambdaExpression lambda => lambda.Body,
            UnaryExpression { NodeType: ExpressionType.Quote, Operand: LambdaExpression lambda } => lambda.Body,
            _ => null
        };
    }

    /// <summary>
    /// Safely extract a MemberExpression
    /// </summary>
    public static MemberExpression? ExtractMemberExpression(Expression expression)
    {
        return expression switch
        {
            MemberExpression member => member,
            UnaryExpression unary when unary.Operand is MemberExpression member2 => member2,
            UnaryExpression unary => ExtractMemberExpression(unary.Operand),
            _ => null
        };
    }

    /// <summary>
    /// Validate argument count
    /// </summary>
    public static void ValidateArgumentCount(string methodName, int actualCount, int expectedMin, int expectedMax = int.MaxValue)
    {
        if (actualCount < expectedMin || actualCount > expectedMax)
        {
            var expectedRange = expectedMax == int.MaxValue ? $"at least {expectedMin}" : $"{expectedMin}-{expectedMax}";
            throw new ArgumentException($"Method '{methodName}' expects {expectedRange} arguments, but got {actualCount}");
        }
    }

    /// <summary>
    /// NULL-safe string conversion
    /// </summary>
    public static string SafeToString(object? value)
    {
        return value switch
        {
            null => "NULL",
            string str => $"'{str}'",
            bool b => b.ToString().ToLower(),
            _ => value.ToString() ?? "NULL"
        };
    }

    /// <summary>
    /// Validate THEN/ELSE type consistency in CASE expressions
    /// </summary>
    public static void ValidateConditionalTypes(Expression ifTrue, Expression ifFalse)
    {
        if (ifTrue.Type != ifFalse.Type)
        {
            throw new NotSupportedException($"CASE expression type mismatch: {ifTrue.Type} and {ifFalse.Type}");
        }
    }

    /// <summary>
    /// Check for forbidden nested aggregate functions
    /// </summary>
    public static void ValidateNoNestedAggregates(Expression expression)
    {
        var visitor = new NestedAggregateDetectionVisitor();
        visitor.Visit(expression);

        if (visitor.HasNestedAggregates)
        {
            throw new NotSupportedException("Nested aggregate functions are not supported");
        }
    }

    /// <summary>
    /// Visitor detecting nested aggregate functions
    /// </summary>
    private class NestedAggregateDetectionVisitor : ExpressionVisitor
    {
        private int _aggregateDepth;
        public bool HasNestedAggregates { get; private set; }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            var methodName = node.Method.Name;

            if (KsqlFunctionRegistry.IsAggregateFunction(methodName))
            {
                if (_aggregateDepth > 0)
                {
                    HasNestedAggregates = true;
                    return node;
                }

                _aggregateDepth++;
                var result = base.VisitMethodCall(node);
                _aggregateDepth--;
                return result;
            }

            return base.VisitMethodCall(node);
        }
    }

    /// <summary>
    /// Visitor for counting nodes
    /// </summary>
    private class NodeCountVisitor : ExpressionVisitor
    {
        public int NodeCount { get; private set; }

        public override Expression? Visit(Expression? node)
        {
            if (node != null)
                NodeCount++;
            return base.Visit(node);
        }
    }
}
