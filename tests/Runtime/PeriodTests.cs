using Kafka.Ksql.Linq.Runtime;
using System;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Runtime;

public class PeriodTests
{
    [Fact]
    public void Period_FloorUtc_Minutes5_Boundary()
    {
        var p = Period.Minutes(5);
        var ts = new DateTime(2025, 8, 23, 10, 7, 3, DateTimeKind.Utc);
        var floored = Periods.FloorUtc(ts, p);
        Assert.Equal(new DateTime(2025, 8, 23, 10, 5, 0, DateTimeKind.Utc), floored);
    }

    [Fact]
    public void Period_FloorUtc_Hours1_Days1_Months1()
    {
        var ts = new DateTime(2025, 8, 23, 10, 7, 3, DateTimeKind.Utc);
        Assert.Equal(new DateTime(2025, 8, 23, 10, 0, 0, DateTimeKind.Utc), Periods.FloorUtc(ts, Period.Hours(1)));
        Assert.Equal(new DateTime(2025, 8, 23, 0, 0, 0, DateTimeKind.Utc), Periods.FloorUtc(ts, Period.Days(1)));
        Assert.Equal(new DateTime(2025, 8, 1, 0, 0, 0, DateTimeKind.Utc), Periods.FloorUtc(ts, Period.Months(1)));
    }

    [Fact]
    public void Period_AddPeriod_MonthOverflow_Handled()
    {
        var ts = new DateTime(2025, 1, 31, 0, 0, 0, DateTimeKind.Utc);
        var added = Periods.AddPeriod(ts, Period.Months(1));
        Assert.Equal(new DateTime(2025, 2, 28, 0, 0, 0, DateTimeKind.Utc), added);
    }

    [Fact]
    public void Period_Weeks1_FloorUtc_AnchorMonday()
    {
        var p = Period.Week(DayOfWeek.Monday);
        var ts = new DateTime(2025, 8, 20, 10, 7, 3, DateTimeKind.Utc); // Wednesday
        var floored = Periods.FloorUtc(ts, p);
        Assert.Equal(new DateTime(2025, 8, 18, 0, 0, 0, DateTimeKind.Utc), floored);
    }

    [Fact]
    public void Period_Weeks1_AddPeriod_SevenDays()
    {
        var ts = new DateTime(2025, 8, 18, 0, 0, 0, DateTimeKind.Utc);
        var added = Periods.AddPeriod(ts, Period.Week());
        Assert.Equal(new DateTime(2025, 8, 25, 0, 0, 0, DateTimeKind.Utc), added);
    }
}
