using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Configuration;
using System;
using System.Threading.Tasks;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Integration;


[Collection("DDL")]
public class InvalidQueryTests
{
    [Theory]
    [Trait("Category", "Integration")]
    [InlineData("SELECT ID, COUNT(*) FROM ORDERS GROUP BY ID;")]
    [InlineData("SELECT CASE WHEN ID=1 THEN 'A' ELSE 2 END FROM ORDERS EMIT CHANGES;")]
    public async Task GeneratedQuery_IsRejected(string ksql)
    {
        try { await EnvInvalidQueryTests.ResetAsync(); } catch { }
        try { await EnvInvalidQueryTests.SetupAsync(); } catch { }

        await using var ctx = EnvInvalidQueryTests.CreateContext();
        var response = await ctx.ExecuteExplainAsync(ksql);
        Assert.False(response.IsSuccess, $"{ksql} unexpectedly succeeded");
    }
}

// local environment helpers
public class EnvInvalidQueryTests
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
