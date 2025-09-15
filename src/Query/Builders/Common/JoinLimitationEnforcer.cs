using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Kafka.Ksql.Linq.Query.Builders.Common;

/// <summary>
/// Class enforcing JOIN limitations
/// Rationale: strictly enforce the two-table limitation in stream processing.
/// </summary>
internal static class JoinLimitationEnforcer
{
    public const int MaxJoinTables = 2;

    /// <summary>
    /// Validate JOIN expression
    /// </summary>
    public static void ValidateJoinExpression(Expression expression)
    {
        var joinCount = CountJoins(expression);
        var tableCount = joinCount + 1;
        if (tableCount > MaxJoinTables)
        {
            throw new StreamProcessingException(
                $"Stream processing supports maximum {MaxJoinTables} table joins. " +
                $"Found {tableCount} tables. " +
                $"Consider data denormalization or use batch processing for complex relationships. " +
                $"Alternative: Create materialized views or use event sourcing patterns.");
        }

        ValidateJoinTypes(expression);
    }

    /// <summary>
    /// Validate JOIN type patterns
    /// </summary>
    public static void ValidateJoinTypes(Expression expression)
    {
        var violations = DetectUnsupportedJoinPatterns(expression);
        if (violations.Any())
        {
            throw new StreamProcessingException(
                $"Unsupported join patterns detected: {string.Join(", ", violations)}. " +
                $"Supported: INNER, LEFT OUTER joins with co-partitioned data.");
        }
    }

    /// <summary>
    /// Count number of JOINs
    /// </summary>
    private static int CountJoins(Expression expression)
    {
        var visitor = new JoinCountVisitor();
        visitor.Visit(expression);
        return visitor.JoinCount;
    }

    /// <summary>
    /// Detect unsupported JOIN patterns
    /// </summary>
    private static List<string> DetectUnsupportedJoinPatterns(Expression expression)
    {
        var visitor = new UnsupportedJoinPatternVisitor();
        visitor.Visit(expression);
        return visitor.Violations;
    }

    /// <summary>
    /// Validate runtime constraints for JOIN execution
    /// </summary>
    public static void ValidateJoinConstraints(
        Type outerType,
        Type innerType,
        Expression outerKeySelector,
        Expression innerKeySelector)
    {
        // Check key type consistency
        var outerKeyType = ExtractKeyType(outerKeySelector);
        var innerKeyType = ExtractKeyType(innerKeySelector);

        if (outerKeyType != null && innerKeyType != null && outerKeyType != innerKeyType)
        {
            throw new StreamProcessingException(
                $"JOIN key types must match. Outer key: {outerKeyType.Name}, Inner key: {innerKeyType.Name}. " +
                $"Ensure both tables are partitioned by the same key type for optimal performance.");
        }

        // Partitioning recommendation warning
        ValidatePartitioningRecommendations(outerType, innerType);
    }

    /// <summary>
    /// Validate partitioning recommendations
    /// </summary>
    private static void ValidatePartitioningRecommendations(Type outerType, Type innerType)
    {
        // Simplified implementation (real environments require detailed partition info)
        var outerTopicName = GetTopicName(outerType);
        var innerTopicName = GetTopicName(innerType);

        if (outerTopicName != null && innerTopicName != null)
        {
            // Warning about partition count and key distribution
            // Normally fetch information from metadata store
            ConsoleWarningIfNeeded(outerTopicName, innerTopicName);
        }
    }

    /// <summary>
    /// Extract key type
    /// </summary>
    private static Type? ExtractKeyType(Expression keySelector)
    {
        if (keySelector is LambdaExpression lambda)
        {
            return lambda.ReturnType;
        }

        return keySelector.Type;
    }

    /// <summary>
    /// Get topic name
    /// </summary>
    private static string? GetTopicName(Type entityType)
    {
        return entityType.Name;
    }

    /// <summary>
    /// Performance warning
    /// </summary>
    private static void ConsoleWarningIfNeeded(string outerTopic, string innerTopic)
    {
        // Output warning in development (use proper logging in production)
        Console.WriteLine($"[KSQL-LINQ WARNING] JOIN performance optimization: " +
            $"Ensure topics '{outerTopic}' and '{innerTopic}' have same partition count and key distribution.");
    }

    /// <summary>
    /// Visitor for counting JOINs
    /// </summary>
    private class JoinCountVisitor : ExpressionVisitor
    {
        public int JoinCount { get; private set; }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node.Method.Name == "Join")
            {
                JoinCount++;

                // Count nested JOINs as well
                if (node.Object != null)
                    Visit(node.Object);

                foreach (var arg in node.Arguments)
                    Visit(arg);

                return node;
            }

            return base.VisitMethodCall(node);
        }
    }

    /// <summary>
    /// Visitor for detecting unsupported JOIN patterns
    /// </summary>
    private class UnsupportedJoinPatternVisitor : ExpressionVisitor
    {
        public List<string> Violations { get; } = new();

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            var methodName = node.Method.Name;

            // Detect JOIN-related methods other than LINQ Join
            switch (methodName)
            {
                case "GroupJoin":
                    Violations.Add("GROUP JOIN (use regular JOIN with GROUP BY instead)");
                    break;

                case "FullOuterJoin":
                    Violations.Add("FULL OUTER JOIN (not supported in KSQL)");
                    break;

                case "RightJoin":
                    Violations.Add("RIGHT JOIN (use LEFT JOIN with swapped operands)");
                    break;

                case "CrossJoin":
                    Violations.Add("CROSS JOIN (performance risk in streaming)");
                    break;
            }

            return base.VisitMethodCall(node);
        }
    }
}
