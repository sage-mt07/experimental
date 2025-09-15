namespace DailyComparisonLib;

using DailyComparisonLib.Models;
using System;
using System.Collections.Generic;
using System.Linq;

public static class Analytics
{
    public static decimal PreviousDayChange(Rate latestRate, decimal previousClose)
    {
        var mid = (latestRate.Bid + latestRate.Ask) / 2m;
        if (previousClose == 0m)
            return 0m;
        return (mid - previousClose) / previousClose;
    }

    public static Dictionary<(string Broker, string Symbol, DateTime Date), decimal> BuildPrevCloseLookup(IEnumerable<RateCandle> dailyBars)
    {
        var lookup = new Dictionary<(string, string, DateTime), decimal>();
        foreach (var group in dailyBars.GroupBy(b => new { b.Broker, b.Symbol }))
        {
            decimal prev = 0m;
            foreach (var bar in group.OrderBy(b => b.BarTime))
            {
                lookup[(bar.Broker, bar.Symbol, bar.BarTime.Date)] = prev;
                prev = bar.Close;
            }
        }
        return lookup;
    }
}
