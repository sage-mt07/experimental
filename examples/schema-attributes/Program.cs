using Kafka.Ksql.Linq;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;
using Kafka.Ksql.Linq.Application;

[KsqlTopic("schema-attributes-demo")]
public class Trade
{
    [KsqlKey(order: 0)] public string Symbol { get; set; } = string.Empty;
    [KsqlDecimal(precision: 18, scale: 4)] public decimal Price { get; set; }
[KsqlTimestamp] public DateTime Timestamp { get; set; }
}

public class SchemaAttrContext : KsqlContext
{
    public SchemaAttrContext(KsqlContextOptions options) : base(options.Configuration!, options.LoggerFactory) { }
    public SchemaAttrContext(Microsoft.Extensions.Configuration.IConfiguration configuration, Microsoft.Extensions.Logging.ILoggerFactory? loggerFactory = null) : base(configuration, loggerFactory) { }
    public EventSet<Trade> Trades { get; set; }
    protected override void OnModelCreating(IModelBuilder b) { }
}

class Program
{
    static async Task Main()
    {
        var cfg = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();
        await using var ctx = new SchemaAttrContext(cfg, LoggerFactory.Create(b => b.AddConsole()));

        await ctx.Trades.AddAsync(new Trade
        {
            Symbol = "FOO",
            Price = 123.4567m,
            Timestamp = DateTime.UtcNow
        });

        await Task.Delay(300);
        using var cts = new System.Threading.CancellationTokenSource(TimeSpan.FromMinutes(5));
        await ctx.Trades.ForEachAsync(t =>
        {
            Console.WriteLine($"Consumed: {t.Symbol} {t.Price} @ {t.Timestamp:O}");
            return Task.CompletedTask;
        }, cancellationToken: cts.Token);
    }
}
