using Kafka.Ksql.Linq;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Application;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

[KsqlTopic("filter-demo")]
public class Event { public int Id { get; set; } public string Category { get; set; } = ""; }

public class FilterContext : KsqlContext
{
    public FilterContext(KsqlContextOptions options) : base(options.Configuration!, options.LoggerFactory) { }
    public FilterContext(Microsoft.Extensions.Configuration.IConfiguration configuration, Microsoft.Extensions.Logging.ILoggerFactory? loggerFactory = null) : base(configuration, loggerFactory) { }
    public EventSet<Event> Events { get; set; }
    protected override void OnModelCreating(IModelBuilder b) { }
}

class Program
{
    static async Task Main()
    {
        var cfg = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();
        await using var ctx = new FilterContext(cfg, LoggerFactory.Create(b => b.AddConsole()));

        await ctx.Events.AddAsync(new Event { Id = 1, Category = "A" });
        await ctx.Events.AddAsync(new Event { Id = 2, Category = "B" });

        await Task.Delay(300);
        using var cts = new System.Threading.CancellationTokenSource(TimeSpan.FromMinutes(5));
        await ctx.Events.ForEachAsync(e =>
        {
            if (e.Category == "A") Console.WriteLine($"A:{e.Id}");
            return Task.CompletedTask;
        }, cancellationToken: cts.Token);
    }
}
