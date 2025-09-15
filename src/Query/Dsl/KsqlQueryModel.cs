using Kafka.Ksql.Linq.Query.Builders;
using Kafka.Ksql.Linq.Query.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Kafka.Ksql.Linq.Query.Dsl;

public class KsqlQueryModel
{
    public Type[] SourceTypes { get; init; } = Array.Empty<Type>();
    public LambdaExpression? JoinCondition { get; set; }
    public LambdaExpression? WhereCondition { get; set; }
    public LambdaExpression? SelectProjection { get; set; }
    public LambdaExpression? GroupByExpression { get; set; }
    public LambdaExpression? HavingCondition { get; set; }
    public Type? BasedOnType { get; set; }
    public LambdaExpression? BasedOnDayKey { get; set; }
    public List<string> BasedOnJoinKeys { get; } = new();
    public string? BasedOnOpen { get; set; }
    public string? BasedOnClose { get; set; }
    public bool BasedOnOpenInclusive { get; set; } = true;
    public bool BasedOnCloseInclusive { get; set; } = false;
    public List<string> Windows { get; } = new();
    public DayOfWeek WeekAnchor { get; set; } = DayOfWeek.Monday;
    public string? TimeKey { get; set; }
    public string? BucketColumnName { get; set; }
    public int? WithinSeconds { get; set; }
    public bool ForbidDefaultWithin { get; set; }
    public int? BaseUnitSeconds { get; set; }
    public int? GraceSeconds { get; set; }
    public LambdaExpression? WhenEmptyFiller { get; set; }
    public System.Collections.Generic.Dictionary<string, object?> Extras { get; } = new();

    public KsqlQueryModel Clone()
    {
        var clone = new KsqlQueryModel
        {
            SourceTypes = (Type[])SourceTypes.Clone(),
            JoinCondition = JoinCondition,
            WhereCondition = WhereCondition,
            SelectProjection = SelectProjection,
            GroupByExpression = GroupByExpression,
            HavingCondition = HavingCondition,
            BasedOnType = BasedOnType,
            BasedOnDayKey = BasedOnDayKey,
            BasedOnOpen = BasedOnOpen,
            BasedOnClose = BasedOnClose,
            BasedOnOpenInclusive = BasedOnOpenInclusive,
            BasedOnCloseInclusive = BasedOnCloseInclusive,
            WeekAnchor = WeekAnchor,
            TimeKey = TimeKey,
            BucketColumnName = BucketColumnName,
            WithinSeconds = WithinSeconds,
            ForbidDefaultWithin = ForbidDefaultWithin,
            BaseUnitSeconds = BaseUnitSeconds,
            GraceSeconds = GraceSeconds,
            WhenEmptyFiller = WhenEmptyFiller
        };
        clone.Windows.AddRange(Windows);
        clone.BasedOnJoinKeys.AddRange(BasedOnJoinKeys);
        foreach (var kv in Extras)
            clone.Extras[kv.Key] = kv.Value;
        return clone;
    }

    /// <summary>
    /// Returns a simple string representation useful for debugging.
    /// </summary>
    public string Dump()
    {
        var sources = string.Join(",", SourceTypes.Select(t => t.Name));
        return $"Sources:[{sources}] Join:{JoinCondition} Where:{WhereCondition} Select:{SelectProjection} Aggregate:{IsAggregateQuery()}";
    }

    public bool HasGroupBy() => GroupByExpression != null;

    public bool HasTumbling() => Windows.Count > 0;

    public bool HasAggregates()
    {
        if (SelectProjection == null) return false;
        var visitor = new AggregateDetectionVisitor();
        visitor.Visit(SelectProjection.Body);
        return visitor.HasAggregates;
    }

    public bool IsAggregateQuery() => HasGroupBy() || HasTumbling() || HasAggregates();

    public StreamTableType DetermineType() => IsAggregateQuery() ? StreamTableType.Table : StreamTableType.Stream;
}
