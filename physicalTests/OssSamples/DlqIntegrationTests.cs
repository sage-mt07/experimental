using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Core.Configuration;
using Kafka.Ksql.Linq.Core.Dlq;
using Xunit;

#nullable enable

namespace Kafka.Ksql.Linq.Tests.Integration;


[Collection("DataRoundTrip")]
public class DlqIntegrationTests
{
    [KsqlTopic("orders_dlq_int")] // use a unique topic to avoid Schema Registry subject conflicts
    public class Order
    {
        public int Id { get; set; }
        [KsqlDecimal(18, 2)]
        public decimal Amount { get; set; }
    }

    public class OrderContext : KsqlContext
    {
        public EventSet<Order> Orders { get; set; } = default!;
        public OrderContext() : base(new KsqlDslOptions()) { }
        public OrderContext(KsqlDslOptions options) : base(options) { }
        protected override void OnModelCreating(IModelBuilder modelBuilder)
        {
            //  modelBuilder.Entity<Order>();
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task ForEachAsync_OnErrorDlq_WritesToDlq()
    {


        //await EnvDlqIntegrationTests.ResetAsync();

        var options = new KsqlDslOptions
        {
            Common = new CommonSection { BootstrapServers = EnvDlqIntegrationTests.KafkaBootstrapServers },
            SchemaRegistry = new SchemaRegistrySection { Url = EnvDlqIntegrationTests.SchemaRegistryUrl }
        };
        // Use test-unique topic to avoid conflicting schemas registered by other tests
        options.Topics.Add("orders_dlq_int", new Configuration.Messaging.TopicSection { Consumer = new Configuration.Messaging.ConsumerSection { AutoOffsetReset = "Earliest", GroupId = Guid.NewGuid().ToString() } });
        options.Topics.Add("dead-letter-queue", new Configuration.Messaging.TopicSection { Consumer = new Configuration.Messaging.ConsumerSection { AutoOffsetReset = "Earliest", GroupId = Guid.NewGuid().ToString() } });

        await using var ctx = new OrderContext(options);
        using (var admin = new Confluent.Kafka.AdminClientBuilder(new Confluent.Kafka.AdminClientConfig { BootstrapServers = EnvDlqIntegrationTests.KafkaBootstrapServers }).Build())
        {
            await PhysicalTestEnv.TopicHelpers.WaitForTopicReady(admin, "orders_dlq_int", 1, 1, TimeSpan.FromSeconds(10));
            await PhysicalTestEnv.TopicHelpers.WaitForTopicReady(admin, "dead-letter-queue", 1, 1, TimeSpan.FromSeconds(10));
        }

        // スキーマ確定用ダミーデータ送信
        await ctx.Orders.AddAsync(new Order { Id = 1, Amount = 0.01m });
        await Task.Delay(5000);
        // DLQ送信テスト本体
        await ctx.Orders
            .OnError(ErrorAction.DLQ)
            .ForEachAsync(_ => throw new Exception("Simulated failure"), TimeSpan.FromSeconds(5));
        await Task.Delay(3000);
        // DLQ検証: 新APIで読み取り
        DlqRecord? found = null;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await foreach (var record in ctx.Dlq.ReadAsync(new DlqReadOptions { FromBeginning = true }, cts.Token))
        {
            if (record.ErrorMessage == "Simulated failure")
            {
                found = record;
                break;
            }
        }

        Assert.NotNull(found);
        Assert.Equal("Simulated failure", found!.ErrorMessage);
        Assert.Equal("Exception", found.ErrorType);
    }
}

// local environment helpers
public class EnvDlqIntegrationTests
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
    internal static Task SetupAsync() => Task.CompletedTask;

    private class BasicContext : KsqlContext
    {
        public BasicContext(KsqlDslOptions options) : base(options) { }
        protected override bool SkipSchemaRegistration => true;
        protected override IEntitySet<T> CreateEntitySet<T>(EntityModel entityModel) => throw new NotImplementedException();
        protected override void OnModelCreating(IModelBuilder modelBuilder) { }
    }
}
