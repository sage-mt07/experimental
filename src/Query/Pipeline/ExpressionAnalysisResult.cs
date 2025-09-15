using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Kafka.Ksql.Linq.Core.Attributes;

namespace Kafka.Ksql.Linq.Query.Pipeline;

/// <summary>
/// Expression analysis result (simplified)
/// </summary>
internal class ExpressionAnalysisResult
{
    public List<MethodCallExpression> MethodCalls { get; set; } = new();
    public bool HasGroupBy => MethodCalls.Any(mc => mc.Method.Name == "GroupBy");
    public bool HasAggregation => MethodCalls.Any(mc => IsAggregateMethod(mc.Method.Name));

    public List<string> Windows { get; } = new();
    public string? TimeKey { get; set; }
    public List<string> GroupByKeys { get; } = new();
    public DayOfWeek WeekAnchor { get; set; } = DayOfWeek.Monday;
    public int? BaseUnitSeconds { get; set; }
    public int? GraceSeconds { get; set; }
    public Dictionary<string, int> GracePerTimeframe { get; } = new();

    public List<string> BasedOnJoinKeys { get; } = new();
    public string? BasedOnOpen { get; set; }
    public string? BasedOnClose { get; set; }
    public bool BasedOnOpenInclusive { get; set; } = true;
    public bool BasedOnCloseInclusive { get; set; } = false;
    public string? BasedOnDayKey { get; set; }
    public Type? PocoType { get; set; }
    public bool WhenEmpty { get; set; }
    public int WindowStartCallCount { get; set; }
    public string? BucketColumnName { get; set; }

    private static bool IsAggregateMethod(string methodName)
    {
        return methodName is "Sum" or "Count" or "Max" or "Min" or "Average" or "Aggregate";
    }

    public QueryMetadata ToMetadata()
    {
        var md = new QueryMetadata(DateTime.UtcNow, "Query") { GraceSeconds = GraceSeconds };
        md = md.WithProperty("timeKey", TimeKey!);
        md = md.WithProperty("basedOn/joinKeys", BasedOnJoinKeys.ToArray());
        md = md.WithProperty("basedOn/openProp", BasedOnOpen!);
        md = md.WithProperty("basedOn/closeProp", BasedOnClose!);
        md = md.WithProperty("basedOn/dayKey", BasedOnDayKey!);
        md = md.WithProperty("basedOn/openInclusive", BasedOnOpenInclusive);
        md = md.WithProperty("basedOn/closeInclusive", BasedOnCloseInclusive);

        if (BucketColumnName != null)
            md = md.WithProperty("bucketColumn", BucketColumnName);

        foreach (var kv in GracePerTimeframe)
            md = md.WithProperty($"grace/{kv.Key}", kv.Value);

        if (PocoType != null && Windows.Count > 0)
        {
            var topicAttr = PocoType.GetCustomAttribute<KsqlTopicAttribute>();
            var baseId = (topicAttr?.Name ?? PocoType.Name).ToLowerInvariant();
            var hub = $"{baseId}_1s_final_s";
            foreach (var tf in Windows)
            {
                md = md.WithProperty($"input/{tf}Live", hub);
                md = md.WithProperty($"input/{tf}Final", hub);
            }
        }
        return md;
    }
}
