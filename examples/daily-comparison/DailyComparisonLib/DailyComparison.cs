namespace DailyComparisonLib.Models;

public class DailyComparison
{
    public string Broker { get; set; } = null!;
    public string Symbol { get; set; } = null!;
    public DateTime Date { get; set; }
    public decimal High { get; set; }
    public decimal Low { get; set; }
    public decimal Close { get; set; }
    public decimal PrevClose { get; set; }
    public decimal Diff { get; set; }
}
