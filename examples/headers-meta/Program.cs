using Kafka.Ksql.Linq;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Application;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

[KsqlTopic("headers-meta-demo")]
public class Msg { public int Id { get; set; } public string Text { get; set; } = ""; }

public class HeadersContext : KsqlContext
{
    public HeadersContext(Microsoft.Extensions.Configuration.IConfiguration configuration, Microsoft.Extensions.Logging.ILoggerFactory? loggerFactory = null)
        : base(configuration, loggerFactory) { }
    public HeadersContext(KsqlContextOptions options) : base(options.Configuration!, options.LoggerFactory) { }
    public EventSet<Msg> Messages { get; set; }
    protected override void OnModelCreating(IModelBuilder b) { }
}

class Program
{
    static async Task Main()
    {
        var cfg = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();
        await using var ctx = new HeadersContext(cfg, LoggerFactory.Create(b => b.AddConsole()));

        var cid = Guid.NewGuid().ToString("N");
        await ctx.Messages.AddAsync(new Msg { Id = 1, Text = "hello" }, new() { ["cid"] = cid });

        await Task.Delay(200);
        using var cts = new System.Threading.CancellationTokenSource(TimeSpan.FromMinutes(5));
        await ctx.Messages.ForEachAsync((m, headers, meta) =>
        {
            Console.WriteLine($"Consumed: {m.Text} cid={headers.GetValueOrDefault("cid")} partition={meta.Partition} offset={meta.Offset}");
            return Task.CompletedTask;
        }, cancellationToken: cts.Token);
    }
}
