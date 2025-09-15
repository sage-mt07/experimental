using System;

namespace Kafka.Ksql.Linq.Runtime;

public enum PeriodUnit
{
    Seconds,
    Minutes,
    Hours,
    Days,
    Months,
    Weeks
}

public readonly struct Period : IEquatable<Period>
{
    public int Value { get; }
    public PeriodUnit Unit { get; }
    public DayOfWeek Anchor { get; }

    private Period(int value, PeriodUnit unit, DayOfWeek anchor = DayOfWeek.Monday)
    {
        Value = value;
        Unit = unit;
        Anchor = anchor;
    }

    public static Period Minutes(int m) => Create(m, PeriodUnit.Minutes);
    public static Period Hours(int h) => Create(h, PeriodUnit.Hours);
    public static Period Days(int d) => Create(d, PeriodUnit.Days);
    public static Period Months(int m) => Create(m, PeriodUnit.Months);
    public static Period Week(DayOfWeek anchor = DayOfWeek.Monday) => Create(1, PeriodUnit.Weeks, anchor);
    public static Period Seconds(int s) => Create(s, PeriodUnit.Seconds);

    private static Period Create(int v, PeriodUnit unit, DayOfWeek anchor = DayOfWeek.Monday)
    {
        if (v <= 0)
            throw new ArgumentOutOfRangeException(nameof(v));
        return new Period(v, unit, anchor);
    }

    public override string ToString() => Unit switch
    {
        PeriodUnit.Seconds => $"{Value}s",
        PeriodUnit.Minutes => $"{Value}m",
        PeriodUnit.Hours => $"{Value}h",
        PeriodUnit.Days => $"{Value}d",
        PeriodUnit.Months => $"{Value}M",
        PeriodUnit.Weeks => "1wk",
        _ => Value.ToString()
    };

    public bool Equals(Period other) => Value == other.Value && Unit == other.Unit && Anchor == other.Anchor;
    public override bool Equals(object? obj) => obj is Period p && Equals(p);
    public override int GetHashCode() => HashCode.Combine(Value, (int)Unit, (int)Anchor);
}

