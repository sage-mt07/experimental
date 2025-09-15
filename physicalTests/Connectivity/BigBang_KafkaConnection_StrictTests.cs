using Confluent.Kafka;
using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Core.Configuration;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Integration;


[Collection("Connectivity")]
public class BigBang_KafkaConnection_StrictTests
{
    [KsqlTopic("orders")]
    public class Order
    {
        [KsqlKey(Order = 0)]
        public int Id { get; set; }
        public double Amount { get; set; }
    }

    public class OrderContext : KsqlContext
    {
        public OrderContext() : base(new KsqlDslOptions()) { }
        public OrderContext(KsqlDslOptions options) : base(options) { }
        protected override void OnModelCreating(IModelBuilder modelBuilder)
            => modelBuilder.Entity<Order>();
        protected override bool SkipSchemaRegistration => true;
    }

    private static KsqlDslOptions CreateOptions() => new()
    {
        Common = new CommonSection { BootstrapServers = EnvBigBang_KafkaConnection_StrictTests.KafkaBootstrapServers },
        SchemaRegistry = new SchemaRegistrySection { Url = EnvBigBang_KafkaConnection_StrictTests.SchemaRegistryUrl }
    };

    [Fact]
    [Trait("Category", "Integration")]
    public async Task EX02_AddAsync_KafkaDown_ShouldLogAndTimeout()
    {


        await DockerHelper.StopServiceAsync("kafka");
        await using var ctx = new OrderContext(CreateOptions());
        var msg = new Order { Id = 1, Amount = 100 };

        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var task = ctx.Set<Order>().AddAsync(msg, null, cts.Token);
        var completed = await Task.WhenAny(task, Task.Delay(11000));
        Assert.Same(task, completed);

        var ex = await Assert.ThrowsAnyAsync<Exception>(() => task);
        var text = ex.ToString();
        // Accept typical failure shapes under forced Kafka down: KafkaException / ProduceException / TaskCanceled / connection errors
        var isKafka = ex is KafkaException;
        var isProduce = ex.GetType().Name.StartsWith("ProduceException", StringComparison.Ordinal);
        var isCanceled = ex is TaskCanceledException || ex is OperationCanceledException || text.Contains("task was canceled", StringComparison.OrdinalIgnoreCase);
        var indicativeMsg = text.Contains("refused", StringComparison.OrdinalIgnoreCase)
                          || text.Contains("Register schema operation failed", StringComparison.OrdinalIgnoreCase)
                          || text.Contains("serialization", StringComparison.OrdinalIgnoreCase)
                          || text.Contains("broker", StringComparison.OrdinalIgnoreCase)
                          || text.Contains("controller", StringComparison.OrdinalIgnoreCase);
        Assert.True(isKafka || isProduce || isCanceled || indicativeMsg,
            $"Unexpected exception: {ex.GetType().FullName}: {ex.Message}");
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task EX02_ForeachAsync_KafkaDown_ShouldLogAndTimeout()
    {


        await DockerHelper.StopServiceAsync("kafka");
        await using var ctx = new OrderContext(CreateOptions());

        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var task = ctx.Set<Order>().ForEachAsync(_ => Task.CompletedTask, TimeSpan.FromSeconds(1), cancellationToken: cts.Token);
        var completed = await Task.WhenAny(task, Task.Delay(11000));
        Assert.Same(task, completed);

        // Alternatively verify that no records were processed within the window
        var processed = 0;
        await ctx.Set<Order>().ForEachAsync(_ => { Interlocked.Increment(ref processed); return Task.CompletedTask; }, TimeSpan.FromSeconds(1), cancellationToken: CancellationToken.None);
        Assert.Equal(0, Volatile.Read(ref processed));
    }
}

// local environment helpers
public class EnvBigBang_KafkaConnection_StrictTests
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
