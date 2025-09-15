namespace DailyComparisonLib.Models;

public class RateCandle
{
    // Broker and Symbol form the composite primary key so they must not be null.
    public string Broker { get; set; } = null!;
    public string Symbol { get; set; } = null!;
    public DateTime BarTime { get; set; }
    public decimal Open { get; set; }
    public decimal High { get; set; }
    public decimal Low { get; set; }
    public decimal Close { get; set; }
}
