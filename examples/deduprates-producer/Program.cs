using System;
using System.Threading.Tasks;
using Kafka.Ksql.Linq;
using Kafka.Ksql.Linq.Core.Abstractions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

using ContractsDedupRate = Examples.Contracts.DedupRateRecord;

public class ProducerContext : KsqlContext
{
    public ProducerContext(IConfiguration cfg, ILoggerFactory? lf = null) : base(cfg, lf) { }

    public EventSet<ContractsDedupRate> ContractsDedupRates { get; set; } = null!;

    protected override void OnModelCreating(IModelBuilder b)
    {
        // Stream/Topic is specified by attributes on the POCO side
    }
}

class Program
{
    static async Task Main()
    {
        var cfg = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();
        var lf = LoggerFactory.Create(b => b.AddConsole());
        var ctx = new ProducerContext(cfg, lf);

        var rnd = new Random();
        var symbols = new[] { "S1", "S2" };
        var broker = "B1";

        for (int i = 0; i < 20; i++)
        {
            var dr = new ContractsDedupRate
            {
                Broker = broker,
                Symbol = symbols[i % symbols.Length],
                Ts = DateTime.UtcNow,
                Bid = Math.Round((decimal)(100 + rnd.NextDouble() * 5), 4)
            };
            await ctx.ContractsDedupRates.AddAsync(dr);
            await Task.Delay(200);
        }

        Console.WriteLine("Produced 20 DedupRate records.");
    }
}
