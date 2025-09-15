using Kafka.Ksql.Linq;
using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Microsoft.Extensions.Logging;
using Confluent.Kafka;

// Manual-commit physical test runner (no test framework)

internal class ManualCommitContext : KsqlContext
{
    public ManualCommitContext(KsqlDslOptions options, ILoggerFactory? loggerFactory = null) : base(options, loggerFactory) { }

    protected override bool SkipSchemaRegistration => true;

    public EventSet<Sample> Samples { get; private set; } = null!;

    protected override void OnModelCreating(IModelBuilder modelBuilder)
        => modelBuilder.Entity<Sample>();

    [KsqlTopic("manual_commit")]
    internal class Sample
    {
        public int Id { get; set; }
    }
}

class Program
{
    static async Task<int> Main()
    {
        var brokers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:39092";
        var schemaRegistryUrl = Environment.GetEnvironmentVariable("SCHEMA_REGISTRY_URL") ?? "http://localhost:18081";
        var ksqlDbUrl = Environment.GetEnvironmentVariable("KSQLDB_URL") ?? "http://localhost:18088";

        Console.WriteLine($"[config] brokers={brokers} sr={schemaRegistryUrl} ksqldb={ksqlDbUrl}");

        var options = new KsqlDslOptions
        {
            Common = new() { BootstrapServers = brokers },
            SchemaRegistry = new() { Url = schemaRegistryUrl },
            KsqlDbUrl = ksqlDbUrl
        };

        // Clean-ish run: unique group id so offset isolation is clear
        var groupId = Guid.NewGuid().ToString();
        options.Topics.Add("manual_commit", new Kafka.Ksql.Linq.Configuration.Messaging.TopicSection
        {
            Consumer = new Kafka.Ksql.Linq.Configuration.Messaging.ConsumerSection
            {
                AutoOffsetReset = "Earliest",
                GroupId = groupId,
                EnableAutoCommit = false,
                AdditionalProperties = new() { ["enable.auto.commit"] = "false" }
            }
        });

        // Produce 5, commit at 3
        var lf = LoggerFactory.Create(b => b.AddConsole());
        await using (var ctx = new ManualCommitContext(options, lf))
        {
            Console.WriteLine("[phase1] producing 5 messages...");
            using var sendCts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            for (var i = 1; i <= 5; i++)
            {
                var attempts = 0;
                while (true)
                {
                    try
                    {
                        await ctx.Samples.AddAsync(new ManualCommitContext.Sample { Id = i }, cancellationToken: sendCts.Token);
                        break;
                    }
                    catch (Confluent.SchemaRegistry.SchemaRegistryException)
                    {
                        if (++attempts >= 3) throw;
                        await Task.Delay(500);
                    }
                }
            }

            Console.WriteLine("[phase1] consuming until Id==3 and committing...");
            using var consumeCts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            await ctx.Samples.ForEachAsync(async (sample, headers, meta) =>
            {
                Console.WriteLine($"[consume] {sample.Id} topic={meta.Topic} p={meta.Partition} off={meta.Offset}");
                if (sample.Id == 3)
                {
                    ctx.Samples.Commit(sample); // manual commit
                    Console.WriteLine($"[commit] committed at Id=3 (topic={meta.Topic} p={meta.Partition} off={meta.Offset})");
                    await Task.Delay(300); // give commit time to flush
                    consumeCts.Cancel();
                }
            }, autoCommit: false, cancellationToken: consumeCts.Token);

            // allow time for commit propagation
            await Task.Delay(1000);

            // Verify committed offset via Kafka consumer API (expect next offset == 3)
            var conf = new ConsumerConfig
            {
                BootstrapServers = brokers,
                GroupId = groupId,
                EnableAutoCommit = false,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
            using (var adminConsumer = new ConsumerBuilder<byte[], byte[]>(conf).Build())
            {
                var tps = new List<TopicPartition> { new TopicPartition("manual_commit", new Partition(0)) };
                var committed = adminConsumer.Committed(tps, TimeSpan.FromSeconds(5));
                var committedOffset = committed.FirstOrDefault().Offset;
                Console.WriteLine($"[debug] committed offset = {committedOffset}");
            }

            // AdminClient-based verification removed for compatibility with current referenced version
        }

        // New context for phase 2 to ensure clean group join
        await using (var ctx = new ManualCommitContext(options, lf))
        {
            Console.WriteLine("[phase2] resume (new ctx); expect Id==4");
            using var nextCts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            ManualCommitContext.Sample? received = null;
            try
            {
                await ctx.Samples.ForEachAsync((sample, _, meta) =>
                {
                    received = sample;
                    ctx.Samples.Commit(sample);
                    Console.WriteLine($"[consume2] {sample.Id} topic={meta.Topic} p={meta.Partition} off={meta.Offset}");
                    nextCts.Cancel();
                    return Task.CompletedTask;
                }, autoCommit: false, cancellationToken: nextCts.Token);
            }
            catch (OperationCanceledException) { }

            if (received?.Id == 4)
            {
                Console.WriteLine("[ok] resumed at Id=4 as expected");
                return 0;
            }
            Console.WriteLine($"[fail] expected Id=4, got {received?.Id}");
            return 1;
        }
    }
}
