using Kafka.Ksql.Linq;
using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Core.Modeling;
using Kafka.Ksql.Linq.Query.Builders;
using System;
using System.Linq;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Integration;

/// <summary>
/// OnModelCreating ベースで、日次ライブと週次ファイナルのモデル→クエリ→マテリアライズ→検証。
/// </summary>
public class BarScheduleDataTests
{
    private class Rate
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime Timestamp { get; set; }
        public double Bid { get; set; }
    }

    private class MarketSchedule
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime Open { get; set; }
        public DateTime Close { get; set; }
        public DateTime MarketDate { get; set; }
    }

    private class Bar1dLive
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlKey(3)] public DateTime BucketStart { get; set; }
        public double Open { get; set; }
        public double High { get; set; }
        public double Low { get; set; }
        public double KsqlTimeFrameClose { get; set; }
    }

    private class Bar1wkFinal
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlKey(3)] public DateTime BucketStart { get; set; }
        public double Open { get; set; }
        public double High { get; set; }
        public double Low { get; set; }
        public double KsqlTimeFrameClose { get; set; }
    }

    private sealed class TestContext : KsqlContext
    {
        public TestContext() : base(new KsqlDslOptions()) { }
        protected override bool SkipSchemaRegistration => true;
        protected override void OnModelCreating(IModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Rate>(readOnly: true);

            // Daily live (1d tumbling within market schedule day)
            modelBuilder.Entity<Bar1dLive>()
                .ToQuery(q => q.From<Rate>()
                    .TimeFrame<MarketSchedule>((r, s) =>
                         r.Broker == s.Broker
                      && r.Symbol == s.Symbol
                      && s.Open <= r.Timestamp && r.Timestamp < s.Close,
                      dayKey: s => s.MarketDate)
                    .Tumbling(r => r.Timestamp, new Kafka.Ksql.Linq.Query.Dsl.Windows { Days = new[] { 1 } })
                    .GroupBy(r => new { r.Broker, r.Symbol })
                    .Select(g => new Bar1dLive
                    {
                        Broker = g.Key.Broker,
                        Symbol = g.Key.Symbol,
                        BucketStart = g.WindowStart(),
                        Open = g.EarliestByOffset(x => x.Bid),
                        High = g.Max(x => x.Bid),
                        Low = g.Min(x => x.Bid),
                        KsqlTimeFrameClose = g.LatestByOffset(x => x.Bid)
                    }));

            // Weekly final (7d tumbling within market schedule week)
            modelBuilder.Entity<Bar1wkFinal>()
                .ToQuery(q => q.From<Rate>()
                    .TimeFrame<MarketSchedule>((r, s) =>
                         r.Broker == s.Broker
                      && r.Symbol == s.Symbol
                      && s.Open <= r.Timestamp && r.Timestamp < s.Close,
                      dayKey: s => s.MarketDate)
                    .Tumbling(r => r.Timestamp, new Kafka.Ksql.Linq.Query.Dsl.Windows { Days = new[] { 7 } })
                    .GroupBy(r => new { r.Broker, r.Symbol })
                    .Select(g => new Bar1wkFinal
                    {
                        Broker = g.Key.Broker,
                        Symbol = g.Key.Symbol,
                        BucketStart = g.WindowStart(),
                        Open = g.EarliestByOffset(x => x.Bid),
                        High = g.Max(x => x.Bid),
                        Low = g.Min(x => x.Bid),
                        KsqlTimeFrameClose = g.LatestByOffset(x => x.Bid)
                    })); // Final系の具体的モードはビルダー側で扱う
        }
    }

    [Fact]
    public void Build_Daily_Live_And_Weekly_Final_CreateSql()
    {
        var ctx = new TestContext();
        var models = ctx.GetEntityModels();

        var daily = models[typeof(Bar1dLive)].QueryModel!;
        Assert.Contains("1d", daily.Windows);
        // Windowed CREATE with tumbling 1d and live (EMIT CHANGES)
        var dailySql = KsqlCreateWindowedStatementBuilder.Build("bar_1d_live", daily, "1d");
        Assert.StartsWith("CREATE TABLE bar_1d_live", dailySql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("WINDOW TUMBLING (SIZE 1 DAYS)", dailySql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("EMIT CHANGES", dailySql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("EARLIEST_BY_OFFSET(Bid)", dailySql);

        var weekly = models[typeof(Bar1wkFinal)].QueryModel!;
        Assert.Contains("7d", weekly.Windows);
        // Windowed CREATE with tumbling 7d and final (EMIT FINAL)
        var weeklySql = KsqlCreateWindowedStatementBuilder.Build("bar_1wk_final", weekly, "7d", emitOverride: "EMIT FINAL");
        Assert.StartsWith("CREATE TABLE bar_1wk_final", weeklySql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("WINDOW TUMBLING (SIZE 7 DAYS)", weeklySql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("EMIT FINAL", weeklySql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("MAX(Bid) AS High", weeklySql);
        Assert.Contains("MIN(Bid) AS Low", weeklySql);
    }
}
