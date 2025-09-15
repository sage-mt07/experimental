using Kafka.Ksql.Linq;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Core.Modeling;
using Kafka.Ksql.Linq.Application;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

[KsqlTopic("orders")]
public class Order 
{ 
    public int Id { get; set; } 
    public int CustomerId { get; set; } 
    public decimal Amount { get; set; } 
}

[KsqlTopic("customers")]
public class Customer 
{ 
    public int Id { get; set; } 
    public string Name { get; set; } = string.Empty; 
    public bool IsActive { get; set; } 
}

public class OrderSummary 
{ 
    public int OrderId { get; set; } 
    public string CustomerName { get; set; } = string.Empty; 
}

public class ViewContext : KsqlContext
{
    public ViewContext(KsqlContextOptions options) : base(options.Configuration!, options.LoggerFactory) { }
    public ViewContext(Microsoft.Extensions.Configuration.IConfiguration configuration, Microsoft.Extensions.Logging.ILoggerFactory? loggerFactory = null) : base(configuration, loggerFactory) { }
    public EventSet<Order> Orders { get; set; }
    public EventSet<Customer> Customers { get; set; }
    public EventSet<OrderSummary> Summaries { get; set; }
    protected override void OnModelCreating(IModelBuilder b)
    {
        // Keep the ToQuery definition as is
        b.Entity<Order>();
        b.Entity<Customer>();
        b.Entity<OrderSummary>().ToQuery(q => q
            .From<Order>()
            .Join<Customer>((o, c) => o.CustomerId == c.Id)
            .Where((o, c) => c.IsActive)
            .Select((o, c) => new OrderSummary { OrderId = o.Id, CustomerName = c.Name }));
    }
}

class Program
{
    static async Task Main()
    {
        var cfg = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();
        await using var ctx = new ViewContext(cfg, LoggerFactory.Create(b => b.AddConsole()));

        // Produce sample rows
        await ctx.Customers.AddAsync(new Customer { Id = 1, Name = "Alice", IsActive = true });
        await ctx.Orders.AddAsync(new Order { Id = 100, CustomerId = 1, Amount = 42.0m });

        await Task.Delay(500);
        using var cts = new System.Threading.CancellationTokenSource(TimeSpan.FromMinutes(5));
        await ctx.Summaries.ForEachAsync(s => { Console.WriteLine($"{s.OrderId}:{s.CustomerName}"); return Task.CompletedTask; }, cancellationToken: cts.Token);
    }
}
