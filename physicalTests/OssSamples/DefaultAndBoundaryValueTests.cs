using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Core.Configuration;
using Xunit;

#nullable enable

namespace Kafka.Ksql.Linq.Tests.Integration;

[Collection("DataRoundTrip")]
public class DefaultAndBoundaryValueTests
{
    [KsqlTopic("alltyperecords")]
    public class AllTypeRecord
    {
        [KsqlKey(Order = 0)]
        public int Id { get; set; }
        public int IntVal { get; set; }
        public long LongVal { get; set; }
        public float FloatVal { get; set; }
        public double DoubleVal { get; set; }
        [KsqlDecimal(18, 6)]
        public decimal DecimalVal { get; set; }
        public string? StringVal { get; set; }
        public bool BoolVal { get; set; }
        public int? NullableIntVal { get; set; }
        [KsqlDecimal(18, 6)]
        public decimal? NullableDecimalVal { get; set; }
    }

    public class AllTypeContext : KsqlContext
    {
        public AllTypeContext(KsqlDslOptions options) : base(options) { }
        protected override void OnModelCreating(IModelBuilder modelBuilder)
        {
            modelBuilder.Entity<AllTypeRecord>();
        }
    }

    private static KsqlDslOptions CreateOptions()
    {
        var options = new KsqlDslOptions
        {
            Common = new CommonSection { BootstrapServers = EnvDefaultAndBoundaryValueTests.KafkaBootstrapServers },
            SchemaRegistry = new SchemaRegistrySection { Url = EnvDefaultAndBoundaryValueTests.SchemaRegistryUrl },
            KsqlDbUrl = EnvDefaultAndBoundaryValueTests.KsqlDbUrl
        };
        // Ensure fresh consumption for each test run
        options.Topics.Add("alltyperecords", new Configuration.Messaging.TopicSection
        {
            Consumer = new Configuration.Messaging.ConsumerSection
            {
                AutoOffsetReset = "Earliest",
                GroupId = Guid.NewGuid().ToString()
            }
        });
        return options;
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task DefaultValuesRoundTrip()
    {


        await EnvDefaultAndBoundaryValueTests.ResetAsync();

        await using var ctx = new AllTypeContext(CreateOptions());
        // Clean and prepare topic
        using (var admin = new Confluent.Kafka.AdminClientBuilder(new Confluent.Kafka.AdminClientConfig { BootstrapServers = EnvDefaultAndBoundaryValueTests.KafkaBootstrapServers }).Build())
        {
            try { await admin.DeleteTopicsAsync(new[] { "alltyperecords" }); } catch { }
            try { await admin.CreateTopicsAsync(new[] { new Confluent.Kafka.Admin.TopicSpecification { Name = "alltyperecords", NumPartitions = 1, ReplicationFactor = 1 } }); } catch { }
            await PhysicalTestEnv.TopicHelpers.WaitForTopicReady(admin, "alltyperecords", 1, 1, TimeSpan.FromSeconds(10));
        }

        var data = new AllTypeRecord { Id = 1 };
        await ctx.Set<AllTypeRecord>().AddAsync(data);

        await ctx.WaitForEntityReadyAsync<AllTypeRecord>(TimeSpan.FromSeconds(5));
        // Topic is 'alltyperecords' -> ksql entity created as ALLTYPERECORDS
        var describe = await ctx.ExecuteStatementAsync("DESCRIBE ALLTYPERECORDS;");
        var desc = describe.Message.ToUpperInvariant();
        Assert.Contains("INTVAL", desc);
        Assert.Contains("LONGVAL", desc);
        Assert.Contains("FLOATVAL", desc);
        Assert.Contains("DOUBLEVAL", desc);
        Assert.Contains("DECIMALVAL", desc);
        Assert.Contains("STRINGVAL", desc);
        Assert.Contains("BOOLVAL", desc);
        Assert.Contains("NULLABLEINTVAL", desc);
        Assert.Contains("NULLABLEDECIMALVAL", desc);

        var list = new List<AllTypeRecord>();
        var cts1 = new System.Threading.CancellationTokenSource(TimeSpan.FromSeconds(10));
        try
        {
            await ctx.Set<AllTypeRecord>().ForEachAsync(r => { list.Add(r); cts1.Cancel(); return Task.CompletedTask; }, cancellationToken: cts1.Token);
        }
        catch (OperationCanceledException) { }

        var result = Assert.Single(list);
        Assert.Equal(data.IntVal, result.IntVal);
        Assert.Equal(data.LongVal, result.LongVal);
        Assert.Equal(data.FloatVal, result.FloatVal);
        Assert.Equal(data.DoubleVal, result.DoubleVal);
        Assert.Equal(data.DecimalVal, result.DecimalVal);
        // Normalized to defined scale (18,6)
        Assert.Equal(6, GetScale(result.DecimalVal));
        Assert.Equal(data.StringVal, result.StringVal);
        Assert.Equal(data.BoolVal, result.BoolVal);
        Assert.Equal(data.NullableIntVal, result.NullableIntVal);
        Assert.Equal(data.NullableDecimalVal, result.NullableDecimalVal);
        if (result.NullableDecimalVal.HasValue)
            Assert.Equal(6, GetScale(result.NullableDecimalVal.Value));
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task DecimalPrecisionRoundTrip()
    {


        await EnvDefaultAndBoundaryValueTests.ResetAsync();

        await using var ctx = new AllTypeContext(CreateOptions());
        using (var admin = new Confluent.Kafka.AdminClientBuilder(new Confluent.Kafka.AdminClientConfig { BootstrapServers = EnvDefaultAndBoundaryValueTests.KafkaBootstrapServers }).Build())
        {
            try { await admin.DeleteTopicsAsync(new[] { "alltyperecords" }); } catch { }
            try { await admin.CreateTopicsAsync(new[] { new Confluent.Kafka.Admin.TopicSpecification { Name = "alltyperecords", NumPartitions = 1, ReplicationFactor = 1 } }); } catch { }
            await PhysicalTestEnv.TopicHelpers.WaitForTopicReady(admin, "alltyperecords", 1, 1, TimeSpan.FromSeconds(10));
        }

        var rows = new[]
        {
            new AllTypeRecord { Id = 1, DecimalVal = 123.456789m },
            new AllTypeRecord { Id = 2, DecimalVal = 0.000m },
            new AllTypeRecord { Id = 3, DecimalVal = 1.000m },
            new AllTypeRecord { Id = 4, DecimalVal = -0.0001m }
        };

        foreach (var r in rows)
            await ctx.Set<AllTypeRecord>().AddAsync(r);

        var list = new List<AllTypeRecord>();
        await ctx.Set<AllTypeRecord>().ForEachAsync(r => { list.Add(r); return Task.CompletedTask; }, TimeSpan.FromSeconds(10));

        Assert.Equal(rows.Length, list.Count);
        // AllTypeRecord.DecimalVal is defined as [KsqlDecimal(18, 6)]
        const int DefinedScale = 6;
        foreach (var r in rows)
        {
            var found = Assert.Single(list, x => x.Id == r.Id);
            // Numeric equality must hold
            Assert.Equal(r.DecimalVal, found.DecimalVal);
            // And recovered value must be normalized to the defined scale (e.g., 0.00 -> 0.000000)
            Assert.Equal(DefinedScale, GetScale(found.DecimalVal));
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task BoundaryValuesRoundTrip()
    {


        await EnvDefaultAndBoundaryValueTests.ResetAsync();

        await using var ctx = new AllTypeContext(CreateOptions());
        using (var admin = new Confluent.Kafka.AdminClientBuilder(new Confluent.Kafka.AdminClientConfig { BootstrapServers = EnvDefaultAndBoundaryValueTests.KafkaBootstrapServers }).Build())
        {
            try { await admin.DeleteTopicsAsync(new[] { "alltyperecords" }); } catch { }
            try { await admin.CreateTopicsAsync(new[] { new Confluent.Kafka.Admin.TopicSpecification { Name = "alltyperecords", NumPartitions = 1, ReplicationFactor = 1 } }); } catch { }
            await PhysicalTestEnv.TopicHelpers.WaitForTopicReady(admin, "alltyperecords", 1, 1, TimeSpan.FromSeconds(10));
        }

        var rows = new[]
        {
            new AllTypeRecord
            {
                Id = 1,
                IntVal = int.MinValue,
                LongVal = long.MinValue,
                FloatVal = float.MinValue,
                DoubleVal = double.MinValue,
                DecimalVal = -999999999999.999999m,
                NullableIntVal = null,
                NullableDecimalVal = null
            },
            new AllTypeRecord
            {
                Id = 2,
                IntVal = int.MaxValue,
                LongVal = long.MaxValue,
                FloatVal = float.MaxValue,
                DoubleVal = double.MaxValue,
                DecimalVal = 999999999999.999999m,
                NullableIntVal = null,
                NullableDecimalVal = null
            }
        };

        foreach (var r in rows)
            await ctx.Set<AllTypeRecord>().AddAsync(r);

        var list = new List<AllTypeRecord>();
        await ctx.Set<AllTypeRecord>().ForEachAsync(r => { list.Add(r); return Task.CompletedTask; }, TimeSpan.FromSeconds(10));

        Assert.Equal(rows.Length, list.Count);
        foreach (var r in rows)
        {
            var found = Assert.Single(list, x => x.Id == r.Id);
            Assert.Equal(r.IntVal, found.IntVal);
            Assert.Equal(r.LongVal, found.LongVal);
            Assert.Equal(r.FloatVal, found.FloatVal);
            Assert.Equal(r.DoubleVal, found.DoubleVal);
            Assert.Equal(r.DecimalVal, found.DecimalVal);
            // Normalized to defined scale (18,6)
            Assert.Equal(6, GetScale(found.DecimalVal));
            Assert.Equal(r.NullableIntVal, found.NullableIntVal);
            Assert.Equal(r.NullableDecimalVal, found.NullableDecimalVal);
        }
    }

    private static int GetScale(decimal value)
    {
        var bits = decimal.GetBits(value);
        return (bits[3] >> 16) & 0x7F;
    }
}


// local environment helpers
public class EnvDefaultAndBoundaryValueTests
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

    internal static async Task ResetAsync()
    {
        // Ensure local services are reachable before starting the test body
        await PhysicalTestEnv.Health.WaitForKafkaAsync(KafkaBootstrapServers, TimeSpan.FromSeconds(120));
        await PhysicalTestEnv.Health.WaitForHttpOkAsync($"{SchemaRegistryUrl}/subjects", TimeSpan.FromSeconds(120));
        await PhysicalTestEnv.Health.WaitForHttpOkAsync($"{KsqlDbUrl}/info", TimeSpan.FromSeconds(120));
    }
    internal static Task SetupAsync() => Task.CompletedTask;

    private class BasicContext : KsqlContext
    {
        public BasicContext(KsqlDslOptions options) : base(options) { }
        protected override bool SkipSchemaRegistration => true;
        protected override IEntitySet<T> CreateEntitySet<T>(EntityModel entityModel) => throw new NotImplementedException();
        protected override void OnModelCreating(IModelBuilder modelBuilder) { }
    }
}
