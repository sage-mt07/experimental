using DailyComparisonLib;
using DailyComparisonLib.Models;
using Kafka.Ksql.Linq.Core.Extensions;

await using var context = MyKsqlContext.FromAppSettings("appsettings.json");

var oneMinBars = await context.Set<RateCandle>().Window(1).ToListAsync();
var fiveMinBars = await context.Set<RateCandle>().Window(5).ToListAsync();
var dailyBars = await context.Set<RateCandle>().Window(1440).ToListAsync();
var prevCloseLookup = Analytics.BuildPrevCloseLookup(dailyBars);
var comparisons = await context.Set<DailyComparison>().ToListAsync();
var latestRates = await context.Set<Rate>().ToListAsync();
var latestLookup = latestRates
    .GroupBy(r => new { r.Broker, r.Symbol })
    .ToDictionary(g => (g.Key.Broker, g.Key.Symbol), g => g.OrderByDescending(r => r.RateTimestamp).First());

Console.WriteLine("--- 1 minute bars ---");
foreach (var c in oneMinBars)
{
    Console.WriteLine($"1m {c.BarTime:t} {c.Symbol} O:{c.Open} H:{c.High} L:{c.Low} C:{c.Close}");
}

Console.WriteLine("--- 5 minute bars ---");
foreach (var c in fiveMinBars)
{
    Console.WriteLine($"5m {c.BarTime:t} {c.Symbol} O:{c.Open} H:{c.High} L:{c.Low} C:{c.Close}");
}

Console.WriteLine("--- Daily bars ---");
foreach (var c in dailyBars)
{
    Console.WriteLine($"day {c.BarTime:d} {c.Symbol} O:{c.Open} H:{c.High} L:{c.Low} C:{c.Close}");
}

Console.WriteLine("--- Daily comparison ---");
foreach (var c in comparisons)
{
    prevCloseLookup.TryGetValue((c.Broker, c.Symbol, c.Date), out var prevClose);
    if (latestLookup.TryGetValue((c.Broker, c.Symbol), out var rate))
    {
        var change = Analytics.PreviousDayChange(rate, prevClose);
        Console.WriteLine($"{c.Date:d} {c.Broker} {c.Symbol} High:{c.High} Low:{c.Low} Close:{c.Close} Diff:{c.Diff} PrevChange:{change:F4}");
    }
    else
    {
        Console.WriteLine($"{c.Date:d} {c.Broker} {c.Symbol} High:{c.High} Low:{c.Low} Close:{c.Close} Diff:{c.Diff} PrevChange:N/A");
    }
}
