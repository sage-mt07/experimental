using Kafka.Ksql.Linq;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Application;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

[KsqlTable]
public class RefData 
{
     public string Key { get; set; } = ""; 
     public string Value { get; set; } = ""; 
}

public class CacheContext : KsqlContext
{
    public CacheContext(KsqlContextOptions options) : base(options.Configuration!, options.LoggerFactory) { }
    public CacheContext(Microsoft.Extensions.Configuration.IConfiguration configuration, Microsoft.Extensions.Logging.ILoggerFactory? loggerFactory = null) : base(configuration, loggerFactory) { }
    public EventSet<RefData> RefDatas { get; set; }
    protected override void OnModelCreating(IModelBuilder b) { }
}

class Program
{
    static async Task Main()
    {
        var cfg = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();
        await using var ctx = new CacheContext(cfg, LoggerFactory.Create(b => b.AddConsole()));

        var rows = await ctx.RefDatas.ToListAsync();
        Console.WriteLine($"Rows: {rows.Count}");
    }
}

