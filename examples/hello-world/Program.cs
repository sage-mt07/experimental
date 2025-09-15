using Kafka.Ksql.Linq;
using Kafka.Ksql.Linq.Core.Abstractions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Application;

[KsqlTopic("hello-world")]
public class HelloMessage
{
    public int Id { get; set; }

    [KsqlTimestamp]
    public DateTime CreatedAt { get; set; }

    public string Text { get; set; } = string.Empty;
}

public class HelloKafkaContext : KsqlContext
{
    public HelloKafkaContext(KsqlContextOptions options) : base(options.Configuration!, options.LoggerFactory) { }
    public HelloKafkaContext(Microsoft.Extensions.Configuration.IConfiguration configuration, Microsoft.Extensions.Logging.ILoggerFactory? loggerFactory = null) : base(configuration, loggerFactory) { }
    public EventSet<HelloMessage> HelloMessages { get; set; } = null!;
    protected override void OnModelCreating(IModelBuilder modelBuilder)
    {
        // Minimal entity registration (Topic/Schema resolved from attributes)
        modelBuilder.Entity<HelloMessage>();
    }
}

class Program
{
    static async Task Main(string[] args)
    {
        var configuration = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .Build();

        await using var context = new HelloKafkaContext(configuration, LoggerFactory.Create(b => b.AddConsole()));

        var message = new HelloMessage
        {
            Id = Random.Shared.Next(),
            CreatedAt = DateTime.UtcNow,
            Text = "Hello World"
        };

        await context.HelloMessages.AddAsync(message);
        // wait until the stream is ready
        await context.WaitForEntityReadyAsync<HelloMessage>(TimeSpan.FromSeconds(5));

        await context.HelloMessages.ForEachAsync(m =>
        {
            Console.WriteLine($"Received: {m.Text}");
            return Task.CompletedTask;
        });
    }
}
