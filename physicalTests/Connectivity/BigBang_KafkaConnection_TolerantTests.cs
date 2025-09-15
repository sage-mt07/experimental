using Confluent.Kafka;
using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Core.Configuration;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Integration;


[Collection("Connectivity")]
public class BigBang_KafkaConnection_TolerantTests
{
    [KsqlTopic("orders")]
    public class Order
    {
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
        Common = new CommonSection { BootstrapServers = EnvBigBang_KafkaConnection_TolerantTests.KafkaBootstrapServers },
        SchemaRegistry = new SchemaRegistrySection { Url = EnvBigBang_KafkaConnection_TolerantTests.SchemaRegistryUrl }
    };

    [Fact]
    [Trait("Category", "Integration")]
    public async Task EX01_AddAsync_KafkaDown_ShouldFailGracefully()
    {


        await DockerHelper.StopServiceAsync("kafka");
        await using var ctx = new OrderContext(CreateOptions());
        var msg = new Order { Id = 1, Amount = 100 };

        var ex = await Assert.ThrowsAnyAsync<Exception>(() => ctx.Set<Order>().AddAsync(msg));
        var text = ex.ToString();
        Assert.True(ex is KafkaException || text.Contains("connection", StringComparison.OrdinalIgnoreCase)
                    || text.Contains("Register schema operation failed", StringComparison.OrdinalIgnoreCase),
            $"Unexpected exception: {ex.GetType().FullName}: {ex.Message}");
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task EX01_ForeachAsync_KafkaDown_ShouldFailGracefully()
    {


        await DockerHelper.StopServiceAsync("kafka");
        await using var ctx = new OrderContext(CreateOptions());

        var processed = 0;
        await ctx.Set<Order>().ForEachAsync(_ => { Interlocked.Increment(ref processed); return Task.CompletedTask; }, TimeSpan.FromSeconds(1));
        Assert.Equal(0, Volatile.Read(ref processed));
    }
}

// local environment helpers
public class EnvBigBang_KafkaConnection_TolerantTests
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
