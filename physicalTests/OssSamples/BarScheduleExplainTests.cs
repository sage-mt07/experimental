using Kafka.Ksql.Linq;
using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Core.Modeling;
using Kafka.Ksql.Linq.Query.Builders;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Integration;

/// <summary>
/// OnModelCreating → ToQuery → Materialize(SQL) → Verify の流れに統一。
/// MarketSchedule 連携の1日/1週バーを検証。
/// </summary>
public class BarScheduleExplainTests
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

    private class Bar1wkLive
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

            modelBuilder.Entity<Bar1dLive>()
                .ToQuery(q => q.From<Rate>()
                    .TimeFrame<MarketSchedule>((r, s) =>
                         r.Broker == s.Broker
                      && r.Symbol == s.Symbol
                      && s.Open <= r.Timestamp && r.Timestamp < s.Close,
                      dayKey: s => s.MarketDate)
                    .Tumbling(r => r.Timestamp, new Query.Dsl.Windows { Hours = new[] { 24 } })
                    .GroupBy(r => new { r.Broker, r.Symbol, BucketStart = r.Timestamp })
                    .Select(g => new Bar1dLive
                    {
                        Broker = g.Key.Broker,
                        Symbol = g.Key.Symbol,
                        BucketStart = g.Key.BucketStart,
                        Open = g.EarliestByOffset(x => x.Bid),
                        High = g.Max(x => x.Bid),
                        Low = g.Min(x => x.Bid),
                        KsqlTimeFrameClose = g.LatestByOffset(x => x.Bid)
                    }));

            modelBuilder.Entity<Bar1wkLive>()
                .ToQuery(q => q.From<Rate>()
                    .TimeFrame<MarketSchedule>((r, s) =>
                         r.Broker == s.Broker
                      && r.Symbol == s.Symbol
                      && s.Open <= r.Timestamp && r.Timestamp < s.Close,
                      dayKey: s => s.MarketDate)
                    .Tumbling(r => r.Timestamp, new Query.Dsl.Windows { Hours = new[] { 24*7 } })
                    .GroupBy(r => new { r.Broker, r.Symbol, BucketStart = r.Timestamp })
                    .Select(g => new Bar1wkLive
                    {
                        Broker = g.Key.Broker,
                        Symbol = g.Key.Symbol,
                        BucketStart = g.Key.BucketStart,
                        Open = g.EarliestByOffset(x => x.Bid),
                        High = g.Max(x => x.Bid),
                        Low = g.Min(x => x.Bid),
                        KsqlTimeFrameClose = g.LatestByOffset(x => x.Bid)
                    }));
        }
    }

    [Fact]
    public void Daily_With_MarketSchedule_Materialize_And_Verify()
    {
        var ctx = new TestContext();
        var m = ctx.GetEntityModels();
        var em = m[typeof(Bar1dLive)];
        Assert.Equal(typeof(MarketSchedule), em.QueryModel!.BasedOnType);
        var sql = KsqlCreateStatementBuilder.Build("bar_1d_live", em.QueryModel!);
        Assert.Contains("EARLIEST_BY_OFFSET(Bid) AS Open", sql);
        Assert.Contains("LATEST_BY_OFFSET(Bid) AS KsqlTimeFrameClose", sql);
        Assert.Contains("CREATE TABLE bar_1d_live", sql);
    }

    [Fact]
    public void Weekly_With_MarketSchedule_Materialize_And_Verify()
    {
        var ctx = new TestContext();
        var m = ctx.GetEntityModels();
        var em = m[typeof(Bar1wkLive)];
        Assert.Equal(typeof(MarketSchedule), em.QueryModel!.BasedOnType);
        var sql = KsqlCreateStatementBuilder.Build("bar_1wk_live", em.QueryModel!);
        Assert.Contains("MAX(Bid) AS High", sql);
        Assert.Contains("MIN(Bid) AS Low", sql);
        Assert.Contains("CREATE TABLE bar_1wk_live", sql);
    }
}
