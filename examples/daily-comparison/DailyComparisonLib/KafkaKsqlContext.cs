using Kafka.Ksql.Linq;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Context;
using Kafka.Ksql.Linq.Core.Modeling;
using DailyComparisonLib.Models;

namespace DailyComparisonLib;

public class KafkaKsqlContext : KafkaContext
{
    public KafkaKsqlContext(KafkaContextOptions options) : base(options)
    {
    }

    protected override void OnModelCreating(IModelBuilder modelBuilder)
    {
        modelBuilder.WithWindow<Rate, MarketSchedule>(
            new[] { 1, 5, 60 },
            r => r.RateTimestamp,
            r => new { r.Broker, r.Symbol, Date = r.RateTimestamp.Date },
            s => new { s.Broker, s.Symbol, s.Date }
        )
        .Select<RateCandle>(w =>
        {
            dynamic key = w.Key;
            return new RateCandle
            {
                Broker = key.Broker,
                Symbol = key.Symbol,
                BarTime = w.BarStart,
                High = w.Source.Max(x => x.Bid),
                Low = w.Source.Min(x => x.Bid),
                Close = w.Source.OrderByDescending(x => x.RateTimestamp).First().Bid,
                Open = w.Source.OrderBy(x => x.RateTimestamp).First().Bid
            };
        });

        modelBuilder.Entity<MarketSchedule>();
        modelBuilder.Entity<RateCandle>();
        modelBuilder.Entity<DailyComparison>();
    }
}
