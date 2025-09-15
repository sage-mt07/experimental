namespace Kafka.Ksql.Linq.Configuration;

using System.Collections.Generic;
using System.Linq;

public class BarLimitOptions
{
    public List<BarLimitEntry> Limits { get; set; } = new();

    public int GetLimit(string symbol, string barType)
    {
        var entry = Limits.FirstOrDefault(e => e.Symbol == symbol && e.BarType == barType);
        return entry?.Limit ?? int.MaxValue;
    }
}

public class BarLimitEntry
{
    public string Symbol { get; set; } = string.Empty;
    public string BarType { get; set; } = string.Empty;
    public int Limit { get; set; }
}
