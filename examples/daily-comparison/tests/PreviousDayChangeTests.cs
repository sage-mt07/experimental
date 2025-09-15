using DailyComparisonLib;
using DailyComparisonLib.Models;
using System;
using Xunit;

namespace DailyComparisonLib.Tests;

public class PreviousDayChangeTests
{
    [Fact]
    public void PreviousDayChange_ComputesExpectedValue()
    {
        var rate = new Rate { Bid = 1.2m, Ask = 1.3m };
        var prevClose = 1.1m;
        var expected = ((1.2m + 1.3m) / 2m - prevClose) / prevClose;
        var result = Analytics.PreviousDayChange(rate, prevClose);
        Assert.Equal(expected, result);
    }

    [Fact]
    public void PreviousDayChange_NoPreviousClose_ReturnsZero()
    {
        var rate = new Rate { Bid = 1.2m, Ask = 1.3m };
        var result = Analytics.PreviousDayChange(rate, 0m);
        Assert.Equal(0m, result);
    }

    [Fact]
    public void BuildPrevCloseLookup_SkipsMissingDays()
    {
        var bars = new[]
        {
            new RateCandle { Broker = "b", Symbol = "s", BarTime = new DateTime(2024,1,1), Close = 1m },
            new RateCandle { Broker = "b", Symbol = "s", BarTime = new DateTime(2024,1,5), Close = 2m }
        };

        var lookup = Analytics.BuildPrevCloseLookup(bars);
        Assert.Equal(0m, lookup[("b", "s", new DateTime(2024,1,1))]);
        Assert.Equal(1m, lookup[("b", "s", new DateTime(2024,1,5))]);
    }
}
