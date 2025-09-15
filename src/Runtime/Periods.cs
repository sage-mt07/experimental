using System;

namespace Kafka.Ksql.Linq.Runtime;

public static class Periods
{
    public static DateTime FloorUtc(DateTime tsUtc, Period p)
    {
        tsUtc = tsUtc.ToUniversalTime();
        return p.Unit switch
        {
            PeriodUnit.Seconds => new DateTime(tsUtc.Year, tsUtc.Month, tsUtc.Day, tsUtc.Hour, tsUtc.Minute, (tsUtc.Second / p.Value) * p.Value, 0, DateTimeKind.Utc),
            PeriodUnit.Minutes => new DateTime(tsUtc.Year, tsUtc.Month, tsUtc.Day, tsUtc.Hour, (tsUtc.Minute / p.Value) * p.Value, 0, 0, DateTimeKind.Utc),
            PeriodUnit.Hours => new DateTime(tsUtc.Year, tsUtc.Month, tsUtc.Day, (tsUtc.Hour / p.Value) * p.Value, 0, 0, DateTimeKind.Utc),
            PeriodUnit.Days => new DateTime(tsUtc.Year, tsUtc.Month, ((tsUtc.Day - 1) / p.Value) * p.Value + 1, 0, 0, 0, DateTimeKind.Utc),
            PeriodUnit.Months => new DateTime(tsUtc.Year, ((tsUtc.Month - 1) / p.Value) * p.Value + 1, 1, 0, 0, 0, DateTimeKind.Utc),
            PeriodUnit.Weeks =>
                tsUtc.Date.AddDays(-((7 + (int)tsUtc.DayOfWeek - (int)p.Anchor) % 7)),
            _ => tsUtc
        };
    }

    public static DateTime AddPeriod(DateTime tsUtc, Period p)
    {
        tsUtc = tsUtc.ToUniversalTime();
        return p.Unit switch
        {
            PeriodUnit.Seconds => tsUtc.AddSeconds(p.Value),
            PeriodUnit.Minutes => tsUtc.AddMinutes(p.Value),
            PeriodUnit.Hours => tsUtc.AddHours(p.Value),
            PeriodUnit.Days => tsUtc.AddDays(p.Value),
            PeriodUnit.Months => tsUtc.AddMonths(p.Value),
            PeriodUnit.Weeks => tsUtc.AddDays(7),
            _ => tsUtc
        };
    }
}

