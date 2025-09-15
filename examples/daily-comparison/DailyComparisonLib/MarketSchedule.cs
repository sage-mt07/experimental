namespace DailyComparisonLib.Models;

public class MarketSchedule
{
    public string Broker { get; set; } = null!;
    public string Symbol { get; set; } = null!;
    public DateTime Date { get; set; }
    public DateTime OpenTime { get; set; }
    public DateTime CloseTime { get; set; }
}
