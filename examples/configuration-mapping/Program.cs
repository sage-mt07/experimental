using Kafka.Ksql.Linq;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Kafka.Ksql.Linq.Application;
using System;
using System.Threading.Tasks;

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
    public EventSet<HelloMessage> HelloMessages { get; set; }
    protected override void OnModelCreating(IModelBuilder modelBuilder) { }
}

class Program
{
    static async Task Main(string[] args)
    {
        var configuration = new ConfigurationBuilder()
            // replace with `appsettings.Development.json` or `appsettings.Production.json`
            .AddJsonFile("appsettings.json")
            .Build();

        var context = KsqlContextBuilder.Create()
            .UseConfiguration(configuration)
            .UseSchemaRegistry(configuration["KsqlDsl:SchemaRegistry:Url"]!)
            .EnableLogging(LoggerFactory.Create(builder => builder.AddConsole()))
            .BuildContext<HelloKafkaContext>();

        var message = new HelloMessage
        {
            Id = Random.Shared.Next(),
            CreatedAt = DateTime.UtcNow,
            Text = "Hello World"
        };

        await context.HelloMessages.AddAsync(message);
        // wait briefly for message to be published
        await Task.Delay(500);

        await context.HelloMessages.ForEachAsync(m =>
        {
            Console.WriteLine($"Received: {m.Text}");
            return Task.CompletedTask;
        });
    }
}
