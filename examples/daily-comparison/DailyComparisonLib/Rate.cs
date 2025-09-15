namespace DailyComparisonLib.Models;

public class Rate
{
    public string Broker { get; set; } = null!;
    public string Symbol { get; set; } = null!;
    public long RateId { get; set; }
    public DateTime RateTimestamp { get; set; }
    public decimal Bid { get; set; }
    public decimal Ask { get; set; }
}
