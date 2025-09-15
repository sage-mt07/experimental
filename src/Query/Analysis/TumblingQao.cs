using System;
using System.Collections.Generic;

namespace Kafka.Ksql.Linq.Query.Analysis;

internal record Timeframe(int Value, string Unit);

internal record BasedOnSpec(
    IReadOnlyList<string> JoinKeys,
    string OpenProp,
    string CloseProp,
    string DayKey,
    bool IsOpenInclusive = true,
    bool IsCloseInclusive = false);

internal class TumblingQao
{
    public string TimeKey { get; init; } = string.Empty;
    public IReadOnlyList<Timeframe> Windows { get; init; } = new List<Timeframe>();
    public IReadOnlyList<string> Keys { get; init; } = new List<string>();
    public IReadOnlyList<string> Projection { get; init; } = new List<string>();
    public IReadOnlyList<ColumnShape> PocoShape { get; init; } = new List<ColumnShape>();
    public BasedOnSpec BasedOn { get; init; } = new(new List<string>(), string.Empty, string.Empty, string.Empty);
    public DayOfWeek WeekAnchor { get; init; } = DayOfWeek.Monday;
    public int? BaseUnitSeconds { get; init; }
    public int? GraceSeconds { get; init; }
    public Dictionary<string, int> GracePerTimeframe { get; } = new();
}
