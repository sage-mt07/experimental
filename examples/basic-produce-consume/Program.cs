using Kafka.Ksql.Linq;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Application;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

[KsqlTopic("basic-produce-consume")]
public class BasicMessage
{
    public int Id { get; set; }

    [KsqlTimestamp]
    public DateTime CreatedAt { get; set; }

    public string Text { get; set; } = string.Empty;
}

public class BasicContext : KsqlContext
{
    public BasicContext(KsqlContextOptions options) : base(options.Configuration!, options.LoggerFactory) { }
    public BasicContext(Microsoft.Extensions.Configuration.IConfiguration configuration, Microsoft.Extensions.Logging.ILoggerFactory? loggerFactory = null) : base(configuration, loggerFactory) { }
    public EventSet<BasicMessage> Messages { get; set; }
    protected override void OnModelCreating(IModelBuilder modelBuilder) { }
}

class Program
{
    static async Task Main(string[] args)
    {
        var configuration = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json")
            .Build();

        await using var context = new BasicContext(configuration, LoggerFactory.Create(b => b.AddConsole()));

        var message = new BasicMessage
        {
            Id = Random.Shared.Next(),
            CreatedAt = DateTime.UtcNow,
            Text = "Basic Flow"
        };

        await context.Messages.AddAsync(message);
        // wait briefly for message to be published
        await Task.Delay(500);

        using var cts = new System.Threading.CancellationTokenSource(TimeSpan.FromMinutes(5));
        await context.Messages.ForEachAsync(m =>
        {
            Console.WriteLine($"Consumed message: {m.Text}");
            return Task.CompletedTask;
        }, cancellationToken: cts.Token);
    }
}
