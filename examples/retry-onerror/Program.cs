using Kafka.Ksql.Linq;
using Kafka.Ksql.Linq.Core.Abstractions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Application;

[KsqlTopic("retry-demo")]
public class Item 
{ 
    public int Id { get; set; } 
    public string Text { get; set; } = ""; 
}

public class RetryContext : KsqlContext
{
    public RetryContext(KsqlContextOptions options) : base(options.Configuration!, options.LoggerFactory) { }
    public RetryContext(Microsoft.Extensions.Configuration.IConfiguration configuration, Microsoft.Extensions.Logging.ILoggerFactory? loggerFactory = null) : base(configuration, loggerFactory) { }
    public EventSet<Item> Items { get; set; }
    protected override void OnModelCreating(IModelBuilder b) { }
}

class Program
{
    static async Task Main()
    {
        var cfg = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();
        await using var ctx = new RetryContext(cfg, LoggerFactory.Create(b => b.AddConsole()));

        var set = ctx.Items.WithRetry(maxRetries: 3, retryInterval: TimeSpan.FromMilliseconds(200))
            .OnError(ErrorAction.DLQ);

        await set.AddAsync(new Item { Id = 1, Text = "Payload" });
        Console.WriteLine("Produced with retry + OnError(Dlq) configured.");
    }
}

