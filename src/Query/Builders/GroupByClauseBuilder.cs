using Kafka.Ksql.Linq.Query.Abstractions;
using Kafka.Ksql.Linq.Query.Builders.Common;
using System;
using System.Linq.Expressions;
using System.Threading;

namespace Kafka.Ksql.Linq.Query.Builders;

/// <summary>
/// Builder for GROUP BY clause content.
/// Rationale: separation-of-concerns; generate only grouping key content without keywords.
/// Example output: "col1, col2" (excluding GROUP BY)
/// </summary>
internal class GroupByClauseBuilder : BuilderBase
{
    private static readonly AsyncLocal<string?> _lastBuiltKeys = new();
    private readonly System.Collections.Generic.IDictionary<string, string>? _paramToSource;
    private readonly bool _forcePrefixAll;

    public GroupByClauseBuilder() { }
    public GroupByClauseBuilder(System.Collections.Generic.IDictionary<string, string> paramToSource)
    {
        _paramToSource = paramToSource;
    }
    public GroupByClauseBuilder(bool forcePrefixAll)
    {
        _forcePrefixAll = forcePrefixAll;
    }
    public GroupByClauseBuilder(System.Collections.Generic.IDictionary<string, string> paramToSource, bool forcePrefixAll)
    {
        _paramToSource = paramToSource;
        _forcePrefixAll = forcePrefixAll;
    }

    internal static string? LastBuiltKeys
    {
        get => _lastBuiltKeys.Value;
        private set => _lastBuiltKeys.Value = value;
    }

    public override KsqlBuilderType BuilderType => KsqlBuilderType.GroupBy;

    protected override KsqlBuilderType[] GetRequiredBuilderTypes()
    {
        return Array.Empty<KsqlBuilderType>(); // No dependency on other builders
    }

    protected override string BuildInternal(Expression expression)
    {
        var visitor = _paramToSource == null
            ? new GroupByExpressionVisitor(forcePrefixAll: _forcePrefixAll)
            : new GroupByExpressionVisitor(_paramToSource, _forcePrefixAll);
        visitor.Visit(expression);

        var result = visitor.GetResult();
        LastBuiltKeys = result;

        if (string.IsNullOrWhiteSpace(result))
        {
            throw new InvalidOperationException("Unable to extract GROUP BY keys from expression");
        }

        return result;
    }

    protected override void ValidateBuilderSpecific(Expression expression)
    {
        // GROUP BY-specific validation
        ValidateNoAggregateInGroupBy(expression);
        ValidateGroupByKeyCount(expression);
    }

    /// <summary>
    /// Disallow aggregate functions in GROUP BY clause
    /// </summary>
    private static void ValidateNoAggregateInGroupBy(Expression expression)
    {
        var visitor = new AggregateDetectionVisitor();
        visitor.Visit(expression);

        if (visitor.HasAggregates)
        {
            throw new InvalidOperationException(
                "Aggregate functions are not allowed in GROUP BY clause");
        }
    }

    /// <summary>
    /// Check limit on number of GROUP BY keys
    /// </summary>
    private static void ValidateGroupByKeyCount(Expression expression)
    {
        var visitor = new GroupByKeyCountVisitor();
        visitor.Visit(expression);

        const int maxKeys = 10; // recommended limit in KSQL
        if (visitor.KeyCount > maxKeys)
        {
            throw new InvalidOperationException(
                $"GROUP BY supports maximum {maxKeys} keys for optimal performance. " +
                $"Found {visitor.KeyCount} keys. Consider using composite keys or data denormalization.");
        }
    }
}
