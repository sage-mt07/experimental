using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Core.Configuration;
using Kafka.Ksql.Linq.Query.Dsl;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Integration;

[Collection("DDL")]
public class JoinIntegrationTests
{
    [KsqlTopic("orders_join")]
    public class OrderValue
    {
        [Kafka.Ksql.Linq.Core.Attributes.KsqlKey]
        public int CustomerId { get; set; }
        public int Id { get; set; }
        public string Region { get; set; } = string.Empty;
        public double Amount { get; set; }
    }

    [KsqlTopic("customers_join")]
    public class Customer
    {
        [Kafka.Ksql.Linq.Core.Attributes.KsqlKey]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [KsqlTopic("orders_customers_join")]
    public class OrderCustomerJoined
    {
        [Kafka.Ksql.Linq.Core.Attributes.KsqlKey]
        public int CustomerId { get; set; }
        public string Name { get; set; } = string.Empty;
        public double Amount { get; set; }
    }


    public class JoinContext : KsqlContext
    {
        // EventSet プロパティで自動登録させる
        public EventSet<Customer> Customers { get; set; }
        public EventSet<OrderValue> OrderValues { get; set; }

        public JoinContext() : base(new KsqlDslOptions()) { }
        public JoinContext(KsqlDslOptions options) : base(options) { }
        public JoinContext(KsqlDslOptions options, ILoggerFactory loggerFactory) : base(options, loggerFactory) { }
        protected override void OnModelCreating(IModelBuilder modelBuilder)
        {
            // ソース
            modelBuilder.Entity<OrderValue>();
            modelBuilder.Entity<Customer>();

            // JOIN定義を QueryModel として登録
            var qm = new KsqlQueryRoot()
                .From<OrderValue>()
                .Join<Customer>((o, c) => o.CustomerId == c.Id)
                .Within(TimeSpan.FromSeconds(300))
                .Select((o, c) => new { o.CustomerId, c.Name, o.Amount })
                .Build();

            var builder = modelBuilder.Entity<OrderCustomerJoined>();
            if (builder is Kafka.Ksql.Linq.Core.Modeling.EntityModelBuilder<OrderCustomerJoined> eb)
            {
                eb.GetModel().QueryModel = qm;
            }
        }
        protected override bool SkipSchemaRegistration => false;
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task TwoTableJoin_Query_ShouldBeValid()
    {


        try
        {
            await EnvJoinIntegrationTests.ResetAsync();
        }
        catch (Exception)
        {
        }

        // ksqlDB が再起動直後でも安定するまで待機（/info相当 + 猶予）
        await PhysicalTestEnv.KsqlHelpers.WaitForKsqlReadyAsync(EnvJoinIntegrationTests.KsqlDbUrl, TimeSpan.FromSeconds(180), graceMs: 2000);

        var options = new KsqlDslOptions
        {
            Common = new CommonSection { BootstrapServers = EnvJoinIntegrationTests.KafkaBootstrapServers },
            SchemaRegistry = new SchemaRegistrySection { Url = EnvJoinIntegrationTests.SchemaRegistryUrl },
            KsqlDbUrl = EnvJoinIntegrationTests.KsqlDbUrl
        };

        using var lf = LoggerFactory.Create(b =>
        {
            b.AddConsole();
            b.SetMinimumLevel(LogLevel.Information);
        });
        await using var ctx = new JoinContext(options, lf);

        // JOIN の計画が生成できることを確認（ソースとJOINは OnModelCreating で設定済み）
        var ksql = "SELECT CustomerId, Name, Amount FROM ORDERS_JOIN JOIN CUSTOMERS_JOIN ON (CustomerId = Id);";
        var response = await ctx.ExecuteExplainAsync(ksql);
        Assert.True(response.IsSuccess, $"{ksql} failed: {response.Message}");
    }

}

// local environment helpers
public class EnvJoinIntegrationTests
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
