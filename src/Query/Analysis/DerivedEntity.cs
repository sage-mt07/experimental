using System;
using System.Collections.Generic;

namespace Kafka.Ksql.Linq.Query.Analysis;

internal enum Role
{
    Live,
    Final,
    Final1s,
    Final1sStream,
    Prev1m,
    Hb,
    Fill
}

internal record ColumnShape(string Name, Type Type, bool IsNullable);

internal class DerivedEntity
{
    public string Id { get; init; } = string.Empty;
    public Role Role { get; init; }
    public Timeframe Timeframe { get; init; } = default!;
    public IReadOnlyList<ColumnShape> KeyShape { get; init; } = Array.Empty<ColumnShape>();
    public IReadOnlyList<ColumnShape> ValueShape { get; init; } = Array.Empty<ColumnShape>();
    public string? TopicHint { get; init; }
    public string? InputHint { get; init; }
    public BasedOnSpec BasedOnSpec { get; init; } = new(new List<string>(), string.Empty, string.Empty, string.Empty);
    public DayOfWeek WeekAnchor { get; init; } = DayOfWeek.Monday;
    public int GraceSeconds { get; init; }
}
