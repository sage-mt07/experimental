using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Configuration;
using System;
using System.Threading.Tasks;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Integration;

#nullable enable

[Collection("Connectivity")]
public class KafkaConnectivityTests
{
    [Fact]
    public async Task ProducerConsumer_RoundTrip()
    {
        await EnvKafkaConnectivityTests.SetupAsync();
        var bootstrap = EnvKafkaConnectivityTests.KafkaBootstrapServers;
        var topic = "connectivity_" + Guid.NewGuid().ToString("N");

        using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build())
        {
            await admin.CreateTopicsAsync(new[] { new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 } });
            await PhysicalTestEnv.TopicHelpers.WaitForTopicReady(admin, topic, 1, 1, TimeSpan.FromSeconds(10));
        }
        // small warm-up to let metadata settle
        await Task.Delay(1000);

        using var producer = new ProducerBuilder<Null, string>(new ProducerConfig { BootstrapServers = bootstrap }).Build();
        // retry produce lightly
        var prodOk = false; Exception? prodEx = null;
        for (var i = 0; i < 3 && !prodOk; i++)
        {
            try
            {
                await producer.ProduceAsync(topic, new Message<Null, string> { Value = "ok" });
                producer.Flush(TimeSpan.FromSeconds(5));
                prodOk = true;
            }
            catch (Exception ex)
            {
                prodEx = ex; await Task.Delay(500);
            }
        }
        if (!prodOk && prodEx != null) throw prodEx;

        using var consumer = new ConsumerBuilder<Null, string>(new ConsumerConfig
        {
            BootstrapServers = bootstrap,
            GroupId = Guid.NewGuid().ToString(),
            AutoOffsetReset = AutoOffsetReset.Earliest
        }).Build();

        consumer.Subscribe(topic);
        // poll a few times until message arrives
        Confluent.Kafka.ConsumeResult<Null, string>? msg = null;
        var end = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (DateTime.UtcNow < end && msg == null)
            msg = consumer.Consume(TimeSpan.FromMilliseconds(500));
        consumer.Close();

        Assert.NotNull(msg);
        Assert.Equal("ok", msg.Message.Value);

        await using var ctx = EnvKafkaConnectivityTests.CreateContext();
        var result = await ctx.ExecuteStatementAsync("SHOW TOPICS;");
        Assert.True(result.IsSuccess);


    }
}


// local environment helpers
public class EnvKafkaConnectivityTests
{
    internal const string SchemaRegistryUrl = "http://127.0.0.1:18081";
    internal const string KsqlDbUrl = "http://127.0.0.1:18088";
    internal const string KafkaBootstrapServers = "127.0.0.1:39092";
    internal const string SkipReason = "Skipped in CI due to missing ksqlDB instance or schema setup failure";

    internal static bool IsKsqlDbAvailable()
    {
        try
        {
            using var ctx = CreateContext();
            var r = ctx.ExecuteStatementAsync("SHOW TOPICS;").GetAwaiter().GetResult();
            return r.IsSuccess;
        }
        catch
        {
            return false;
        }
    }

    internal static KsqlContext CreateContext()
    {
        var options = new KsqlDslOptions
        {
            Common = new CommonSection { BootstrapServers = KafkaBootstrapServers },
            SchemaRegistry = new SchemaRegistrySection { Url = SchemaRegistryUrl },
            KsqlDbUrl = KsqlDbUrl
        };
        return new BasicContext(options);
    }

    internal static Task ResetAsync() => Task.CompletedTask;
    internal static async Task SetupAsync()
    {
        await PhysicalTestEnv.Health.WaitForKafkaAsync(KafkaBootstrapServers, TimeSpan.FromSeconds(120));
        await PhysicalTestEnv.Health.WaitForHttpOkAsync($"{SchemaRegistryUrl}/subjects", TimeSpan.FromSeconds(120));
        // Prefer robust ksqlDB stabilization: /healthcheck + SHOW QUERIES consecutive OK
        await PhysicalTestEnv.KsqlHelpers.WaitForKsqlStableAsync(KsqlDbUrl, consecutiveOk: 5, timeout: TimeSpan.FromSeconds(180), settleMs: 30000);
    }

    private class BasicContext : KsqlContext
    {
        public BasicContext(KsqlDslOptions options) : base(options) { }
        protected override bool SkipSchemaRegistration => true;
        protected override IEntitySet<T> CreateEntitySet<T>(EntityModel entityModel) => throw new NotImplementedException();
        protected override void OnModelCreating(IModelBuilder modelBuilder) { }
    }
}
