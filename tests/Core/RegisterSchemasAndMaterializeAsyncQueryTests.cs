using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Modeling;
using Kafka.Ksql.Linq.Mapping;
using Kafka.Ksql.Linq.Query.Dsl;
using Kafka.Ksql.Linq.Infrastructure.KsqlDb;
using Kafka.Ksql.Linq.Tests;
using Kafka.Ksql.Linq.Core.Extensions;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using System.Linq;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Core;

public class RegisterSchemasAndMaterializeAsyncQueryTests
{
    private class CapturingClient : IKsqlDbClient
    {
        public ConcurrentQueue<string> Statements { get; } = new();
        public string Topic { get; set; } = string.Empty;
        public Task<KsqlDbResponse> ExecuteStatementAsync(string statement)
        {
            Statements.Enqueue(statement);
            if (statement.StartsWith("SHOW QUERIES", StringComparison.OrdinalIgnoreCase))
                return Task.FromResult(new KsqlDbResponse(true, $"Q1|{Topic}|PERSISTENT|RUNNING"));
            if (statement.StartsWith("SHOW TOPICS", StringComparison.OrdinalIgnoreCase))
                return Task.FromResult(new KsqlDbResponse(true, $"{Topic}|1"));
            return Task.FromResult(new KsqlDbResponse(true, string.Empty));
        }
        public Task<KsqlDbResponse> ExecuteExplainAsync(string ksql) => Task.FromResult(new KsqlDbResponse(true, string.Empty));
        public Task<HashSet<string>> GetTableTopicsAsync() => Task.FromResult(new HashSet<string>());
        public Task<int> ExecuteQueryStreamCountAsync(string sql, TimeSpan? timeout = null) => Task.FromResult(0);
        public Task<int> ExecutePullQueryCountAsync(string sql, TimeSpan? timeout = null) => Task.FromResult(0);
    }

    private class ListLogger : ILogger
    {
        public IDisposable BeginScope<TState>(TState state) where TState : notnull => null!;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) { }
    }

    private class DummyContext : KsqlContext
    {
        private DummyContext() : base(new Microsoft.Extensions.Configuration.ConfigurationBuilder().Build()) { }
    }

    private static DummyContext CreateContext(CapturingClient client, ListLogger logger, ConcurrentDictionary<Type, EntityModel> models, Confluent.SchemaRegistry.ISchemaRegistryClient schema)
    {
        var ctx = (DummyContext)RuntimeHelpers.GetUninitializedObject(typeof(DummyContext));
        var dsl = new KsqlDslOptions();
        DefaultValueBinder.ApplyDefaults(dsl);
        typeof(KsqlContext).GetField("_dslOptions", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(ctx, dsl);
        typeof(KsqlContext).GetField("_mappingRegistry", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(ctx, new MappingRegistry());
        typeof(KsqlContext).GetField("_entityModels", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(ctx, models);
        typeof(KsqlContext).GetField("_ksqlDbClient", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(ctx, client);
        typeof(KsqlContext).GetField("_logger", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(ctx, logger);
        typeof(KsqlContext).GetField("_schemaRegistryClient", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(ctx, new Lazy<Confluent.SchemaRegistry.ISchemaRegistryClient>(() => schema));
        return ctx;
    }

    private class Source { public int Id { get; set; } }
    private class Target { public int Id { get; set; } }
    private class AggTarget { public int Id { get; set; } public int Count { get; set; } }

    [Fact]
    public async Task RegisterSchemasAndMaterializeAsync_ProcessesQueryEntity()
    {
        var client = new CapturingClient();
        var logger = new ListLogger();
        var models = new ConcurrentDictionary<Type, EntityModel>();
        var schema = new FakeSchemaRegistryClient();
        var ctx = CreateContext(client, logger, models, schema);

        var builder = new ModelBuilder();
        builder.Entity<Target>().ToQuery(q => q.From<Source>().Select(s => new Target { Id = s.Id }));

        using (Kafka.Ksql.Linq.Core.Modeling.ModelCreatingScope.Enter())
        {
            PrivateAccessor.InvokePrivate(ctx, "ApplyModelBuilderSettings", new[] { typeof(ModelBuilder) }, args: new object[] { builder });
        }

        Assert.True(models.ContainsKey(typeof(Target)));

        using (Kafka.Ksql.Linq.Core.Modeling.ModelCreatingScope.Enter())
        {
            var task = (Task)PrivateAccessor.InvokePrivate(ctx, "RegisterSchemasAndMaterializeAsync", Type.EmptyTypes, args: Array.Empty<object>())!;
            await task;
        }

        Assert.Equal(2, client.Statements.Count);
        Assert.Contains("CREATE STREAM", client.Statements.ToArray()[0]);
        Assert.Contains("INSERT INTO", client.Statements.ToArray()[1]);
    }

    [Fact]
    public async Task RegisterSchemasAndMaterializeAsync_ProcessesGroupedQueryWithCtas()
    {
        var client = new CapturingClient();
        var logger = new ListLogger();
        var models = new ConcurrentDictionary<Type, EntityModel>();
        var schema = new FakeSchemaRegistryClient();
        var ctx = CreateContext(client, logger, models, schema);

        var builder = new ModelBuilder();
        builder.Entity<AggTarget>().ToQuery(q => q
            .From<Source>()
            .GroupBy(s => s.Id)
            .Select(g => new AggTarget { Id = g.Key, Count = g.Count() }));

        using (Kafka.Ksql.Linq.Core.Modeling.ModelCreatingScope.Enter())
        {
            PrivateAccessor.InvokePrivate(ctx, "ApplyModelBuilderSettings", new[] { typeof(ModelBuilder) }, args: new object[] { builder });
        }

        client.Topic = models[typeof(AggTarget)].GetTopicName();

        using (Kafka.Ksql.Linq.Core.Modeling.ModelCreatingScope.Enter())
        {
            var task = (Task)PrivateAccessor.InvokePrivate(ctx, "RegisterSchemasAndMaterializeAsync", Type.EmptyTypes, args: Array.Empty<object>())!;
            await task;
        }

        var statements = client.Statements.ToArray();
        Assert.Equal(3, statements.Length);
        Assert.Contains("CREATE TABLE", statements[0]);
        Assert.Contains("AS", statements[0]);
        Assert.Contains("SHOW QUERIES", statements[1]);
        Assert.Contains("SHOW TOPICS", statements[2]);
        Assert.DoesNotContain("INSERT INTO", string.Join("\n", statements));
    }
}
