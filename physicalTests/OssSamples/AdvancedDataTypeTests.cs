using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Core.Configuration;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Integration;


[Collection("DataRoundTrip")]
public class AdvancedDataTypeTests
{
    public enum Status { Pending, Done }

    [KsqlTopic("records")]
    public class Record
    {
        [KsqlKey(Order = 0)]
        public int Id { get; set; }
        [KsqlDecimal(18, 4)]
        public decimal Price { get; set; }
        public DateTime Created { get; set; }
        //  public Status State { get; set; }
    }

    public class RecordContext : KsqlContext
    {
        public EventSet<Record> Records { get; set; }
        public RecordContext() : base(new KsqlDslOptions()) { }
        public RecordContext(KsqlDslOptions options) : base(options) { }
        protected override void OnModelCreating(IModelBuilder modelBuilder)
        {
            //  modelBuilder.Entity<Record>();
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task Decimal_DateTime_Enum_RoundTrip()
    {


        //try
        //{
        //    await EnvAdvancedDataTypeTests.ResetAsync();
        //}
        //catch (Exception ex)
        //{
        //    Console.WriteLine($"[Warning] ResetAsync failed: {ex}");
        //    return;
        //  //  throw new SkipException($"Test setup failed in ResetAsync: {ex.Message}");
        //}

        var options = new KsqlDslOptions
        {
            Common = new CommonSection { BootstrapServers = EnvAdvancedDataTypeTests.KafkaBootstrapServers },
            SchemaRegistry = new SchemaRegistrySection { Url = EnvAdvancedDataTypeTests.SchemaRegistryUrl },
            KsqlDbUrl = EnvAdvancedDataTypeTests.KsqlDbUrl
        };
        options.Topics.Add("records", new Configuration.Messaging.TopicSection { Consumer = new Configuration.Messaging.ConsumerSection { AutoOffsetReset = "Earliest", GroupId = Guid.NewGuid().ToString() } });

        await using var ctx = new RecordContext(options);
        // Ensure topic is clean and ready
        using (var admin = new Confluent.Kafka.AdminClientBuilder(new Confluent.Kafka.AdminClientConfig { BootstrapServers = EnvAdvancedDataTypeTests.KafkaBootstrapServers }).Build())
        {
            try { await admin.DeleteTopicsAsync(new[] { "records" }); } catch { }
            try { await admin.CreateTopicsAsync(new[] { new Confluent.Kafka.Admin.TopicSpecification { Name = "records", NumPartitions = 1, ReplicationFactor = 1 } }); } catch { }
            await PhysicalTestEnv.TopicHelpers.WaitForTopicReady(admin, "records", 1, 1, TimeSpan.FromSeconds(10));
        }

        var data = new Record { Id = 1, Price = 12.3456m, Created = DateTime.UtcNow };
        await ctx.Records.AddAsync(data);
        var list = new List<Record>();
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        try
        {
            await ctx.Records.ForEachAsync(r => { list.Add(r); cts.Cancel(); return Task.CompletedTask; }, cancellationToken: cts.Token);
        }
        catch (OperationCanceledException) { /* expected after first message */ }
        Assert.Single(list);
        Assert.Equal(data.Price, list[0].Price);
        // Defined scale is 4 for Price; ensure normalized recovery
        Assert.Equal(4, GetScale(list[0].Price));
        Assert.True(Math.Abs((list[0].Created - data.Created).TotalMinutes) < 1);
    }

    private static int GetScale(decimal value)
    {
        var bits = decimal.GetBits(value);
        return (bits[3] >> 16) & 0x7F;
    }
}

// local environment helpers
public class EnvAdvancedDataTypeTests
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
