namespace Kafka.Ksql.Linq.Cache.Configuration;

using System.Collections.Generic;

internal class TableCacheOptions
{
    public List<TableCacheEntry> Entries { get; set; } = new();
}

internal class TableCacheEntry
{
    public string Entity { get; set; } = string.Empty;
    public string SourceTopic { get; set; } = string.Empty;
    public bool EnableCache { get; set; } = true;
    public string? StoreName { get; set; }
    public string? BaseDirectory { get; set; }
}
