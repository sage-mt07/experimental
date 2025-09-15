using Kafka.Ksql.Linq;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Application;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

[KsqlTopic("manual-commit-orders")]
public class ManualCommitOrder
{
    public int OrderId { get; set; }
    public decimal Amount { get; set; }
}

public class ManualCommitContext : KsqlContext
{
    public ManualCommitContext(KsqlContextOptions options) : base(options.Configuration!, options.LoggerFactory) { }
    public ManualCommitContext(Microsoft.Extensions.Configuration.IConfiguration configuration, Microsoft.Extensions.Logging.ILoggerFactory? loggerFactory = null) : base(configuration, loggerFactory) { }
    public EventSet<ManualCommitOrder> Orders { get; set; }
    protected override void OnModelCreating(IModelBuilder modelBuilder) { }
}

class Program
{
    static async Task Main(string[] args)
    {
        var configuration = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json")
            .Build();

        await using var context = new ManualCommitContext(configuration, LoggerFactory.Create(b => b.AddConsole()));

        var order = new ManualCommitOrder
        {
            OrderId = Random.Shared.Next(),
            Amount = 10m
        };

        await context.Orders.AddAsync(order);
        await Task.Delay(500);

        await context.Orders.ForEachAsync(async (order, headers, meta) =>
        {
            Console.WriteLine($"Processing order {order.OrderId}: {order.Amount}");
            context.Orders.Commit(order);
        }, autoCommit: false);
    }
}
