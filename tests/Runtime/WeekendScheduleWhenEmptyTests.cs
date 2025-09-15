using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;
using Xunit.Abstractions;

namespace Kafka.Ksql.Linq.Tests.Runtime;

public class WeekendScheduleWhenEmptyTests
{
    private readonly ITestOutputHelper _out;
    public WeekendScheduleWhenEmptyTests(ITestOutputHelper output) => _out = output;

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
        public DateTime MarketDate => OpenTimeUtc.Date;
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
    public void Schedule_Excludes_Weekend_WhenEmpty_Fills_Weekday_Only()
    {
        var broker = "B1"; var symbol = "S1";
        // Assume Fri=2025-09-05, Sat=2025-09-06, Sun=2025-09-07, Mon=2025-09-08
        var fri = new DateTime(2025, 9, 5, 0, 0, 0, DateTimeKind.Utc);
        var sat = fri.AddDays(1);
        var sun = fri.AddDays(2);
        var mon = fri.AddDays(3);

        // Sessions: Fri 00:00-00:10, Mon 00:00-00:10 (Sat/Sun 休場)
        var friSch = new MarketSchedule { Broker = broker, Symbol = symbol, OpenTimeUtc = fri, CloseTimeUtc = fri.AddMinutes(10) };
        var monSch = new MarketSchedule { Broker = broker, Symbol = symbol, OpenTimeUtc = mon, CloseTimeUtc = mon.AddMinutes(10) };

        // Ticks: FriとMonに1秒+0.01。Sat/Sunは生成してもTimeFrame(=sch範囲)で除外される想定
        var ticks = new List<Tick>();
        void AddTicks(DateTime start)
        {
            var cur = start; decimal px = 100m;
            for (int s = 0; s < 600; s++)
            {
                // 2分目を欠損→WhenEmpty 確認
                if (!(s >= 60 && s < 120))
                    ticks.Add(new Tick { Broker = broker, Symbol = symbol, TimestampUtc = cur, Bid = Math.Round(px, 4, MidpointRounding.AwayFromZero) });
                cur = cur.AddSeconds(1); px += 0.01m;
            }
        }
        AddTicks(fri);
        AddTicks(mon);
        // ついでに土日のデータも投入（しかしスケジュール外なので無視される）
        ticks.Add(new Tick { Broker = broker, Symbol = symbol, TimestampUtc = sat.AddMinutes(1), Bid = 999m });
        ticks.Add(new Tick { Broker = broker, Symbol = symbol, TimestampUtc = sun.AddMinutes(1), Bid = 999m });

        _out.WriteLine("[ticks] samples around Fri and Mon (weekend ignored by TimeFrame)");
        foreach (var t in ticks.Where(t => t.TimestampUtc < fri.AddMinutes(1)).Take(5))
            _out.WriteLine($"F {t.TimestampUtc:MM-dd HH:mm:ss} {t.Bid:F4}");
        foreach (var t in ticks.Where(t => t.TimestampUtc >= mon && t.TimestampUtc < mon.AddMinutes(1)).Take(5))
            _out.WriteLine($"M {t.TimestampUtc:MM-dd HH:mm:ss} {t.Bid:F4}");

        var friBars = Build1mBarsWithWhenEmpty(broker, symbol, friSch, ticks);
        var monBars = Build1mBarsWithWhenEmpty(broker, symbol, monSch, ticks);
        Assert.Equal(10, friBars.Count);
        Assert.Equal(10, monBars.Count);

        // 土日セッションは無い→結合後の全体バーにも土日の分は存在しない（TimeFrame 効いている前提）
        var allBars = new List<Rate>();
        allBars.AddRange(friBars);
        allBars.AddRange(monBars);
        Assert.DoesNotContain(allBars, b => b.BucketStart.Date == sat.Date || b.BucketStart.Date == sun.Date);

        // Friの2分目が WhenEmpty 補完されていること
        Assert.Equal(friBars[0].Close, friBars[1].Open);
        Assert.Equal(friBars[1].Open, friBars[1].High);
        Assert.Equal(friBars[1].Open, friBars[1].Low);
        Assert.Equal(friBars[1].Open, friBars[1].Close);

        // 5分ロールアップも平日のみ生成される
        var fri5 = friBars.GroupBy(b => Floor5m(b.BucketStart)).Select(g => g.Key).ToList();
        var mon5 = monBars.GroupBy(b => Floor5m(b.BucketStart)).Select(g => g.Key).ToList();
        Assert.All(fri5, d => Assert.Equal(DayOfWeek.Friday, d.DayOfWeek));
        Assert.All(mon5, d => Assert.Equal(DayOfWeek.Monday, d.DayOfWeek));
    }
}

