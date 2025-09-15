using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Core.Configuration;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Integration;

[Collection("DataRoundTrip")]
public class NoKeyPocoTests
{
    [KsqlTopic("records_no_key")]
    public class Record
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    public class RecordContext : KsqlContext
    {
        public EventSet<Record> Records { get; set; }
        public RecordContext() : base(new KsqlDslOptions()) { }
        public RecordContext(KsqlDslOptions options) : base(options) { }
        protected override void OnModelCreating(IModelBuilder modelBuilder)
        {
            //modelBuilder.Entity<Record>();
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task SendAndReceive_NoKeyRecord()
    {


        //await EnvNoKeyPocoTests.ResetAsync();

        var options = new KsqlDslOptions
        {
            Common = new CommonSection { BootstrapServers = EnvNoKeyPocoTests.KafkaBootstrapServers },
            SchemaRegistry = new SchemaRegistrySection { Url = EnvNoKeyPocoTests.SchemaRegistryUrl },
            KsqlDbUrl = EnvNoKeyPocoTests.KsqlDbUrl,
            Topics = new Dictionary<string, Configuration.Messaging.TopicSection>()
        };
        options.Topics.Add("records_no_key", new Configuration.Messaging.TopicSection { Consumer = new Configuration.Messaging.ConsumerSection { AutoOffsetReset = "Earliest", GroupId = Guid.NewGuid().ToString() } });
        await using var ctx = new RecordContext(options);
        // Ensure topic is clean and ready
        using (var admin = new Confluent.Kafka.AdminClientBuilder(new Confluent.Kafka.AdminClientConfig { BootstrapServers = EnvNoKeyPocoTests.KafkaBootstrapServers }).Build())
        {
            try { await admin.DeleteTopicsAsync(new[] { "records_no_key" }); } catch { }
            try { await admin.CreateTopicsAsync(new[] { new Confluent.Kafka.Admin.TopicSpecification { Name = "records_no_key", NumPartitions = 1, ReplicationFactor = 1 } }); } catch { }
            await PhysicalTestEnv.TopicHelpers.WaitForTopicReady(admin, "records_no_key", 1, 1, TimeSpan.FromSeconds(10));
        }

        var data = new Record { Id = 1, Name = "alice" };
        await ctx.Records.AddAsync(data);
        // Give the broker and consumer subscription a brief moment to settle
        await Task.Delay(1000);

        var list = new List<Record>();
        var cts = new System.Threading.CancellationTokenSource(TimeSpan.FromSeconds(10));
        try
        {
            await ctx.Records.ForEachAsync(r => { list.Add(r); cts.Cancel(); return Task.CompletedTask; }, cancellationToken: cts.Token);
        }
        catch (OperationCanceledException) { /* expected after first message */ }

        Assert.Single(list);
        Assert.Equal(data.Id, list[0].Id);
        Assert.Equal(data.Name, list[0].Name);

        await ctx.DisposeAsync();
    }
}

// local environment helpers
static class EnvNoKeyPocoTests
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
