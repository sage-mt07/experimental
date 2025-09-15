using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Core.Configuration;
using Xunit;

#nullable enable

namespace Kafka.Ksql.Linq.Tests.Integration;

[Collection("DataRoundTrip")]
public class PrimingBehaviorTests
{
    [KsqlTopic("priming_records")]
    public class Record
    {
        [KsqlKey(Order = 0)]
        public int Id { get; set; }
        public string? Note { get; set; }
    }

    public class RecordContext : KsqlContext
    {
        public RecordContext(KsqlDslOptions options) : base(options) { }
        protected override void OnModelCreating(IModelBuilder modelBuilder)
            => modelBuilder.Entity<Record>();
    }

    private static KsqlDslOptions CreateOptions()
    {
        var options = new KsqlDslOptions
        {
            Common = new CommonSection { BootstrapServers = EnvPrimingBehaviorTests.KafkaBootstrapServers },
            SchemaRegistry = new SchemaRegistrySection { Url = EnvPrimingBehaviorTests.SchemaRegistryUrl },
            KsqlDbUrl = EnvPrimingBehaviorTests.KsqlDbUrl
        };
        options.Topics.Add("priming_records", new Kafka.Ksql.Linq.Configuration.Messaging.TopicSection
        {
            Consumer = new Kafka.Ksql.Linq.Configuration.Messaging.ConsumerSection
            {
                AutoOffsetReset = "Earliest",
                GroupId = Guid.NewGuid().ToString()
            }
        });
        return options;
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task AlwaysPriming_DoesNotLeakIntoConsumption()
    {
        await using var ctx = new RecordContext(CreateOptions());

        await ctx.WaitForEntityReadyAsync<Record>(TimeSpan.FromSeconds(10));

        // Send a real record
        await ctx.Set<Record>().AddAsync(new Record { Id = 1, Note = "real" });

        var list = new List<Record>();
        await ctx.Set<Record>().ForEachAsync(r => { list.Add(r); return Task.CompletedTask; }, TimeSpan.FromSeconds(5));

        Assert.Single(list);
        Assert.Equal(1, list[0].Id);
        Assert.Equal("real", list[0].Note);
    }
}

public class EnvPrimingBehaviorTests
{
    internal const string SchemaRegistryUrl = "http://127.0.0.1:18081";
    internal const string KsqlDbUrl = "http://127.0.0.1:18088";
    internal const string KafkaBootstrapServers = "127.0.0.1:39092";
}
