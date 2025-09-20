using Confluent.Kafka;
using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Configuration;
using System;
using System.Net.Http;
using System.Threading.Tasks;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Integration;

[TestCaseOrderer("Kafka.Ksql.Linq.Tests.Integration.PriorityOrderer", "Kafka.Ksql.Linq.Tests.Integration")]
[Collection("Connectivity")]
public class PortConnectivityTests
{
    [Fact]
    //[TestPriority(1)]
    public async Task Kafka_Broker_Should_Be_Reachable()
    {
        await EnvPortConnectivityTests.SetupAsync();
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = EnvPortConnectivityTests.KafkaBootstrapServers }).Build();
        var meta = admin.GetMetadata(TimeSpan.FromSeconds(10));
        Assert.NotEmpty(meta.Brokers);
    }

    [Fact]
    //[TestPriority(2)]
    public async Task SchemaRegistry_Should_Be_Reachable()
    {
        await EnvPortConnectivityTests.SetupAsync();
        using var http = new HttpClient();
        var resp = await http.GetAsync($"{EnvPortConnectivityTests.SchemaRegistryUrl}/subjects");
        Assert.True(resp.IsSuccessStatusCode);
    }

    [Fact]
    //[TestPriority(3)]
    public async Task KsqlDb_Should_Be_Reachable()
    {
        await EnvPortConnectivityTests.SetupAsync();
        await using var ctx = EnvPortConnectivityTests.CreateContext();
        var result = await ctx.ExecuteStatementAsync("SHOW TOPICS;");
        Assert.True(result.IsSuccess);
    }
}

// local environment helpers
static class EnvPortConnectivityTests
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
        await PhysicalTestEnv.Health.WaitForHttpOkAsync($"{KsqlDbUrl}/info", TimeSpan.FromSeconds(120));
    }

    private class BasicContext : KsqlContext
    {
        public BasicContext(KsqlDslOptions options) : base(options) { }
        protected override bool SkipSchemaRegistration => true;
        protected override IEntitySet<T> CreateEntitySet<T>(EntityModel entityModel) => throw new NotImplementedException();
        protected override void OnModelCreating(IModelBuilder modelBuilder) { }
    }
}
