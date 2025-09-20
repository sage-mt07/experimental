using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Core.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Integration;

[Collection("DataRoundTrip")]
public class RocksDbToListAsyncTests
{
    [KsqlTopic("rocks_to_list_ticks")]
    [KsqlTable]
    public class Tick
    {
        [KsqlKey(0)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(1)] public string Symbol { get; set; } = string.Empty;
        public double Price { get; set; }
    }

    public class TickContext : KsqlContext
    {
        public EventSet<Tick> Ticks { get; set; } = default!;
        public TickContext() : base(new KsqlDslOptions()) { }
        public TickContext(KsqlDslOptions options, ILoggerFactory loggerFactory) : base(options, loggerFactory) { }
        protected override void OnModelCreating(IModelBuilder modelBuilder)
        {
            // modelBuilder.Entity<Tick>();
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task ToListAsync_Reads_From_RocksDb_Cache()
    {
        var options = new KsqlDslOptions
        {
            Common = new CommonSection { BootstrapServers = EnvRocksDbTests.KafkaBootstrapServers },
            SchemaRegistry = new SchemaRegistrySection { Url = EnvRocksDbTests.SchemaRegistryUrl },
            KsqlDbUrl = EnvRocksDbTests.KsqlDbUrl
        };
        options.Entities.Add(new EntityConfiguration { Entity = nameof(Tick), EnableCache = true, SourceTopic = "rocks_to_list_ticks" });
        options.Topics.Add("rocks_to_list_ticks", new Kafka.Ksql.Linq.Configuration.Messaging.TopicSection
        {
            Consumer = new Kafka.Ksql.Linq.Configuration.Messaging.ConsumerSection { AutoOffsetReset = "Earliest", GroupId = Guid.NewGuid().ToString() },
            Creation = new Kafka.Ksql.Linq.Configuration.Messaging.TopicCreationSection { NumPartitions = 1, ReplicationFactor = 1 }
        });

        // Ensure topic exists
        using (var admin = new Confluent.Kafka.AdminClientBuilder(new Confluent.Kafka.AdminClientConfig { BootstrapServers = EnvRocksDbTests.KafkaBootstrapServers }).Build())
        {
            try { await admin.DeleteTopicsAsync(new[] { "rocks_to_list_ticks" }); } catch { }
            try { await admin.CreateTopicsAsync(new[] { new Confluent.Kafka.Admin.TopicSpecification { Name = "rocks_to_list_ticks", NumPartitions = 1, ReplicationFactor = 1 } }); } catch { }
            await PhysicalTestEnv.TopicHelpers.WaitForTopicReady(admin, "rocks_to_list_ticks", 1, 1, TimeSpan.FromSeconds(10));
        }

        var loggerFactory = LoggerFactory.Create(builder =>
        {
            builder
                .SetMinimumLevel(LogLevel.Information)
                .AddConsole();
        });

        await using var ctx = new TickContext(options, loggerFactory);

        // Produce a few records
        await ctx.Ticks.AddAsync(new Tick { Broker = "B1", Symbol = "S1", Price = 100 });
        await ctx.Ticks.AddAsync(new Tick { Broker = "B1", Symbol = "S2", Price = 200 });

        // Allow cache to start and materialize
        await Task.Delay(TimeSpan.FromSeconds(5));

        var list = await ctx.Ticks.ToListAsync();
        Assert.True(list.Count >= 2, $"expected >=2 rows, got {list.Count}");
        Assert.Contains(list, t => t.Broker == "B1" && t.Symbol == "S1" && t.Price == 100);
        Assert.Contains(list, t => t.Broker == "B1" && t.Symbol == "S2" && t.Price == 200);
    }
}

public static class EnvRocksDbTests
{
    internal const string SchemaRegistryUrl = "http://127.0.0.1:18081";
    internal const string KafkaBootstrapServers = "127.0.0.1:39092";
    internal const string KsqlDbUrl = "http://127.0.0.1:18088";
}

