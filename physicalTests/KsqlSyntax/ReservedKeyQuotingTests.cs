using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Query.Ddl;
using Kafka.Ksql.Linq.Query.Pipeline;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Integration;

public class ReservedKeyQuotingTests
{
    private sealed class SimpleContext : KsqlContext
    {
        public SimpleContext(KsqlDslOptions opt) : base(opt) { }
        protected override bool SkipSchemaRegistration => true;
        protected override void OnModelCreating(IModelBuilder modelBuilder) { }
    }

    private static async Task<KsqlContext> CreateReadyContextAsync()
    {
        var options = new KsqlDslOptions
        {
            Common = new CommonSection { BootstrapServers = "127.0.0.1:39092" },
            SchemaRegistry = new Kafka.Ksql.Linq.Core.Configuration.SchemaRegistrySection { Url = "http://127.0.0.1:18081" },
            KsqlDbUrl = "http://127.0.0.1:18088"
        };
        await PhysicalTestEnv.KsqlHelpers.WaitForKsqlReadyAsync(options.KsqlDbUrl!, TimeSpan.FromSeconds(120));
        return new SimpleContext(options);
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task CreateStream_WithReservedKeyFields_ShouldSucceed()
    {
        await using var ctx = await CreateReadyContextAsync();

        var schema = new DdlSchemaBuilder("dlq_reserved_test", DdlObjectType.Stream, "dlq-reserved-test", 1, 1)
            .AddColumn("Topic", "VARCHAR", isKey: true)
            .AddColumn("Partition", "INT", isKey: true)
            .AddColumn("Offset", "BIGINT", isKey: true)
            .AddColumn("ErrorMessage", "VARCHAR")
            .Build();
        var gen = new DDLQueryGenerator();
        string sql;
        using (Kafka.Ksql.Linq.Core.Modeling.ModelCreatingScope.Enter())
        {
            sql = gen.GenerateCreateStream(new SchemaProvider(schema));
        }

        var resp = await ctx.ExecuteStatementAsync(sql);
        Assert.True(resp.IsSuccess, resp.Message);

        // cleanup best-effort
        await ctx.ExecuteStatementAsync("DROP STREAM IF EXISTS dlq_reserved_test DELETE TOPIC;");
    }

    private sealed class SchemaProvider : IDdlSchemaProvider
    {
        private readonly DdlSchemaDefinition _schema;
        public SchemaProvider(DdlSchemaDefinition s) => _schema = s;
        public DdlSchemaDefinition GetSchema() => _schema;
    }
}

