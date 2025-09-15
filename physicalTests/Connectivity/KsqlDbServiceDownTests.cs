using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Configuration;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Integration;


[Collection("Connectivity")]
public class KsqlDbServiceDownTests
{
    [Fact]
    [Trait("Category", "Integration")]
    public async Task ExecuteStatement_ShouldFail_WhenKsqlDbDown()
    {

        try
        {
            await EnvKsqlDbServiceDownTests.ResetAsync();

        }
        catch (Exception)
        {
        }
        await DockerHelper.StopServiceAsync("ksqldb-server");

        await using var ctx = EnvKsqlDbServiceDownTests.CreateContext();
        await Assert.ThrowsAsync<HttpRequestException>(async () =>
        {
            await ctx.ExecuteStatementAsync("SHOW TOPICS;");
        });

        await DockerHelper.StartServiceAsync("ksqldb-server");
        await EnvKsqlDbServiceDownTests.SetupAsync();
    }
}

// local environment helpers
public class EnvKsqlDbServiceDownTests
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
