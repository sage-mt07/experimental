using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;
using Xunit.Abstractions;

namespace Kafka.Ksql.Linq.Tests.Runtime;

public class WhenEmptyScheduleTests
{
    private readonly ITestOutputHelper _out;
    public WhenEmptyScheduleTests(ITestOutputHelper output) => _out = output;

    public sealed class Tick
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime TimestampUtc { get; set; }
        public decimal Bid { get; set; }
    }

    public sealed class MarketSchedule
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime OpenTimeUtc { get; set; }
        public DateTime CloseTimeUtc { get; set; }
    }

    public sealed class Rate
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime BucketStart { get; set; }
        public decimal Open { get; set; }
        public decimal High { get; set; }
        public decimal Low { get; set; }
        public decimal Close { get; set; }
    }

    private static DateTime FloorMin(DateTime dt) => new(((dt.Ticks / TimeSpan.TicksPerMinute) * TimeSpan.TicksPerMinute), DateTimeKind.Utc);
    private static DateTime Floor5m(DateTime dt) => new(((dt.Ticks / TimeSpan.FromMinutes(5).Ticks) * TimeSpan.FromMinutes(5).Ticks), DateTimeKind.Utc);

    private static List<Rate> Build1mBarsWithWhenEmpty(string broker, string symbol, MarketSchedule sch, IEnumerable<Tick> ticks)
    {
        var src = ticks.Where(t => t.Broker == broker && t.Symbol == symbol)
                       .Where(t => t.TimestampUtc >= sch.OpenTimeUtc && t.TimestampUtc < sch.CloseTimeUtc)
                       .OrderBy(t => t.TimestampUtc)
                       .ToList();
        var start = FloorMin(sch.OpenTimeUtc);
        var end = FloorMin(sch.CloseTimeUtc);
        var minutes = new List<DateTime>();
        for (var t = start; t < end; t = t.AddMinutes(1)) minutes.Add(t);
        var bars = new List<Rate>();
        Rate? prev = null; int idx = 0;
        foreach (var m in minutes)
        {
            var next = m.AddMinutes(1);
            var group = new List<Tick>();
            while (idx < src.Count && src[idx].TimestampUtc >= m && src[idx].TimestampUtc < next)
            { group.Add(src[idx]); idx++; }
            if (group.Count > 0)
            {
                var o = group.First().Bid; var h = group.Max(x => x.Bid); var l = group.Min(x => x.Bid); var c = group.Last().Bid;
                var bar = new Rate { Broker = broker, Symbol = symbol, BucketStart = m, Open = o, High = h, Low = l, Close = c };
                bars.Add(bar); prev = bar;
            }
            else if (prev != null)
            {
                var c = prev.Close;
                var bar = new Rate { Broker = broker, Symbol = symbol, BucketStart = m, Open = c, High = c, Low = c, Close = c };
                bars.Add(bar); prev = bar;
            }
        }
        return bars;
    }

    [Fact]
    public void OneMinute_And_FiveMinute_WhenEmpty_Work_With_MissingTicks()
    {
        var broker = "B1"; var symbol = "S1"; var day = new DateTime(2025, 9, 6, 0, 0, 0, DateTimeKind.Utc);
        var sch = new MarketSchedule { Broker = broker, Symbol = symbol, OpenTimeUtc = day, CloseTimeUtc = day.AddMinutes(10) };

        var ticks = new List<Tick>();
        var cursor = sch.OpenTimeUtc; decimal price = 100m;
        for (int s = 0; s < 600; s++)
        {
            if (!(s >= 60 && s < 120)) // 2分目のティックを欠損
                ticks.Add(new Tick { Broker = broker, Symbol = symbol, TimestampUtc = cursor, Bid = Math.Round(price, 4, MidpointRounding.AwayFromZero) });
            cursor = cursor.AddSeconds(1); price += 0.01m;
        }

        _out.WriteLine("[ticks] first 80 seconds (missing 60..119)");
        foreach (var t in ticks.Where(t => t.TimestampUtc < sch.OpenTimeUtc.AddSeconds(80)))
            _out.WriteLine($"{t.TimestampUtc:HH:mm:ss} {t.Bid:F4}");

        var bars1 = Build1mBarsWithWhenEmpty(broker, symbol, sch, ticks);
        Assert.Equal(10, bars1.Count);
        // 2分目（Open+1分）が WhenEmpty により前分Closeで埋まる
        var m1 = bars1[1];
        Assert.Equal(bars1[0].Close, m1.Open);
        Assert.Equal(m1.Open, m1.High);
        Assert.Equal(m1.Open, m1.Low);
        Assert.Equal(m1.Open, m1.Close);

        var bars5 = bars1
            .GroupBy(b => Floor5m(b.BucketStart))
            .OrderBy(g => g.Key)
            .Select(g => new Rate
            {
                Broker = broker,
                Symbol = symbol,
                BucketStart = g.Key,
                Open = g.OrderBy(x => x.BucketStart).First().Open,
                High = g.Max(x => x.High),
                Low = g.Min(x => x.Low),
                Close = g.OrderBy(x => x.BucketStart).Last().Close
            }).ToList();

        Assert.Equal(2, bars5.Count);
        _out.WriteLine("[1m bars] first 3");
        foreach (var b in bars1.Take(3)) _out.WriteLine($"{b.BucketStart:HH:mm} O:{b.Open} H:{b.High} L:{b.Low} C:{b.Close}");
        _out.WriteLine("[5m bars]");
        foreach (var b in bars5) _out.WriteLine($"[5m] {b.BucketStart:HH:mm} O:{b.Open} H:{b.High} L:{b.Low} C:{b.Close}");
    }
}

