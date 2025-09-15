using Kafka.Ksql.Linq;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;
using Kafka.Ksql.Linq.Application;

[KsqlTopic("orders")]
public class Order
{
    public int Id { get; set; }

    [KsqlDecimal(precision: 18, scale: 2)]
    public decimal Amount { get; set; }
}

public class OrderContext : KsqlContext
{
    public OrderContext(KsqlContextOptions options) : base(options.Configuration!, options.LoggerFactory) { }
    public OrderContext(Microsoft.Extensions.Configuration.IConfiguration configuration, Microsoft.Extensions.Logging.ILoggerFactory? loggerFactory = null) : base(configuration, loggerFactory) { }
    public EventSet<Order> Orders { get; set; }
    protected override void OnModelCreating(IModelBuilder modelBuilder) { }
}

class Program
{
    static async Task Main(string[] args)
    {
        var configuration = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json")
            .Build();

        await using var context = new OrderContext(configuration, LoggerFactory.Create(b => b.AddConsole()));

        var order = new Order
        {
            Id = Random.Shared.Next(),
            Amount = -42.5m
        };

        await context.Orders.AddAsync(order);
        await Task.Delay(500);

        await context.Orders
            .OnError(ErrorAction.DLQ)
            .WithRetry(3)
            .ForEachAsync(o =>
            {
                if (o.Amount < 0)
                {
                    throw new InvalidOperationException("Amount cannot be negative");
                }
                Console.WriteLine($"Processed order {o.Id}: {o.Amount}");
                return Task.CompletedTask;
            });
    }
}
