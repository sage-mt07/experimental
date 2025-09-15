using DailyComparisonLib.Models;
using Kafka.Ksql.Linq;

namespace DailyComparisonLib;

public class ScheduleUpdater
{
    private readonly KafkaKsqlContext _context;
    public ScheduleUpdater(KafkaKsqlContext context)
    {
        _context = context;
    }

    public async Task UpdateAsync(IEnumerable<MarketSchedule> schedules, CancellationToken ct = default)
    {
        foreach (var s in schedules)
        {
            await _context.Set<MarketSchedule>().AddAsync(s, ct);
        }
    }
}
