using DailyComparisonLib.Models;
using Kafka.Ksql.Linq;
using Kafka.Ksql.Linq.Core.Extensions;
using Kafka.Ksql.Linq.Configuration;
using System.Linq;

namespace DailyComparisonLib;

public class Aggregator
{
    private readonly KafkaKsqlContext _context;
    private readonly BarLimitOptions _limitOptions;

    public Aggregator(KafkaKsqlContext context, BarLimitOptions? limitOptions = null)
    {
        _context = context;
        _limitOptions = limitOptions ?? new BarLimitOptions();
    }

    public async Task<(List<DailyComparison> DailyBars, List<RateCandle> MinuteBars)> AggregateAsync(DateTime date, CancellationToken ct = default)
    {
        var dailyBars = (await _context.Set<DailyComparison>().ToListAsync(ct))
            .Where(d => d.Date == date.Date)
            .ToList();

        var candleSet = _context.Set<RateCandle>();
        var model = candleSet.GetEntityModel();
        if (model.BarTimeSelector == null)
            throw new InvalidOperationException("RateCandle model is missing bar time selector.");

        var barTime = (Func<RateCandle, DateTime>)model.BarTimeSelector.Compile();

        var minuteBars = (await candleSet.ToListAsync(ct))
            .Where(c => barTime(c).Date == date.Date)
            .GroupBy(c => c.Symbol)
            .SelectMany(g =>
            {
                var limit = _limitOptions.GetLimit(g.Key, nameof(RateCandle));
                return g.OrderByDescending(barTime).Take(limit);
            })
            .ToList();

        return (dailyBars, minuteBars);
    }
}
