using DailyComparisonLib.Models;

namespace DailyComparisonLib;

public static class RateGenerator
{
    private static readonly Random _rand = new();

    public static Rate Create(string broker, string symbol, long id, DateTime timestamp)
    {
        var price = (decimal)(_rand.NextDouble() * 100 + 50);
        return new Rate
        {
            Broker = broker,
            Symbol = symbol,
            RateId = id,
            RateTimestamp = timestamp,
            Bid = price,
            Ask = price + 0.1m
        };
    }
}
