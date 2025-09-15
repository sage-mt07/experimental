using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Infrastructure.KsqlDb;
using Kafka.Ksql.Linq.Mapping;
using Kafka.Ksql.Linq.Query.Abstractions;
using Kafka.Ksql.Linq.Query.Dsl;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Core;

public class EnsureQueryEntityDdlAsyncTests
{
    private class CapturingClient : IKsqlDbClient
    {
        public List<string> Statements { get; } = new();
        public string Topic { get; set; } = "tgt";
        public Task<KsqlDbResponse> ExecuteStatementAsync(string statement)
        {
            Statements.Add(statement);
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
        public List<string> Messages { get; } = new();
        public IDisposable BeginScope<TState>(TState state) where TState : notnull => null!;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => Messages.Add(formatter(state, exception));
    }

    private class DummyContext : KsqlContext
    {
        private DummyContext() : base(new Microsoft.Extensions.Configuration.ConfigurationBuilder().Build()) { }
    }

    private static DummyContext CreateContext(CapturingClient client, ListLogger logger, ConcurrentDictionary<Type, EntityModel> models)
    {
        var ctx = (DummyContext)RuntimeHelpers.GetUninitializedObject(typeof(DummyContext));
        var dsl = new KsqlDslOptions();
        DefaultValueBinder.ApplyDefaults(dsl);
        typeof(KsqlContext).GetField("_dslOptions", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(ctx, dsl);
        typeof(KsqlContext).GetField("_mappingRegistry", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(ctx, new MappingRegistry());
        typeof(KsqlContext).GetField("_entityModels", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(ctx, models);
        typeof(KsqlContext).GetField("_ksqlDbClient", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(ctx, client);
        typeof(KsqlContext).GetField("_logger", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(ctx, logger);
        return ctx;
    }

    private class Source { public int Id { get; set; } }
    private class Target { public int Id { get; set; } }

    [Fact]
    public async Task LogsCreateThenInsert()
    {
        var client = new CapturingClient();
        var logger = new ListLogger();
        var models = new ConcurrentDictionary<Type, EntityModel>();

        models[typeof(Source)] = new EntityModel
        {
            EntityType = typeof(Source),
            TopicName = "src",
            KeyProperties = new[] { typeof(Source).GetProperty(nameof(Source.Id))! },
            AllProperties = typeof(Source).GetProperties()
        };

        var targetModel = new EntityModel
        {
            EntityType = typeof(Target),
            TopicName = "tgt",
            QueryModel = new KsqlQueryRoot().From<Source>().Select(s => new { s.Id }).Build(),
            KeyProperties = new[] { typeof(Target).GetProperty(nameof(Target.Id))! },
            AllProperties = typeof(Target).GetProperties(),
            KeySchemaFullName = "k",
            ValueSchemaFullName = "v"
        };
        models[typeof(Target)] = targetModel;

        var ctx = CreateContext(client, logger, models);

        using (Kafka.Ksql.Linq.Core.Modeling.ModelCreatingScope.Enter())
        {
            var task = (Task)PrivateAccessor.InvokePrivate(ctx, "EnsureQueryEntityDdlAsync", new[] { typeof(Type), typeof(EntityModel) }, args: new object[] { typeof(Target), targetModel })!;
            await task;
        }

        var ddlMsgs = logger.Messages.Where(m => !m.StartsWith("ksql execute:", StringComparison.OrdinalIgnoreCase)).ToList();
        Assert.Equal(2, ddlMsgs.Count);
        Assert.Contains("CREATE STREAM", ddlMsgs[0]);
        Assert.Contains("INSERT INTO", ddlMsgs[1]);
        Assert.DoesNotContain("CREATE TABLE AS", string.Join("\n", ddlMsgs));
        Assert.Equal(2, client.Statements.Count);
        Assert.Contains("CREATE STREAM", client.Statements[0]);
        Assert.Contains("INSERT INTO", client.Statements[1]);
        Assert.Contains("EMIT CHANGES", client.Statements[1]);
    }

    [Fact]
    public async Task GroupByQuery_IgnoresExplicitStream_LogsCtas()
    {
        var client = new CapturingClient();
        var logger = new ListLogger();
        var models = new ConcurrentDictionary<Type, EntityModel>();

        models[typeof(Source)] = new EntityModel
        {
            EntityType = typeof(Source),
            TopicName = "src",
            KeyProperties = new[] { typeof(Source).GetProperty(nameof(Source.Id))! },
            AllProperties = typeof(Source).GetProperties()
        };

        var targetModel = new EntityModel
        {
            EntityType = typeof(Target),
            TopicName = "tgt",
            QueryModel = new KsqlQueryRoot().From<Source>().GroupBy(s => s.Id).Select(g => new { Id = g.Key, Count = g.Count() }).Build(),
            KeyProperties = new[] { typeof(Target).GetProperty(nameof(Target.Id))! },
            AllProperties = typeof(Target).GetProperties(),
            KeySchemaFullName = "k",
            ValueSchemaFullName = "v",
            Partitions = 1
        };
        targetModel.SetStreamTableType(StreamTableType.Stream);
        models[typeof(Target)] = targetModel;

        var ctx = CreateContext(client, logger, models);

        using (Kafka.Ksql.Linq.Core.Modeling.ModelCreatingScope.Enter())
        {
            var task = (Task)PrivateAccessor.InvokePrivate(ctx, "EnsureQueryEntityDdlAsync", new[] { typeof(Type), typeof(EntityModel) }, args: new object[] { typeof(Target), targetModel })!;
            await task;
        }

        var ddlMsgs = logger.Messages.Where(m => !m.StartsWith("ksql execute:", StringComparison.OrdinalIgnoreCase)).ToList();
        Assert.Single(ddlMsgs);
        Assert.Contains("CREATE TABLE", ddlMsgs[0]);
        Assert.Contains("AS", ddlMsgs[0]);
        Assert.Equal(3, client.Statements.Count);
        Assert.Contains("CREATE TABLE", client.Statements[0]);
        Assert.Contains("SHOW QUERIES", client.Statements[1]);
        Assert.Contains("SHOW TOPICS", client.Statements[2]);
        Assert.DoesNotContain("INSERT INTO", string.Join("\n", client.Statements));

        DecimalPrecisionConfig.Configure(18, 2, null);
    }

    [Fact]
    public async Task ExplicitTableWithoutAggregation_LogsCreateThenInsert()
    {
        var client = new CapturingClient();
        var logger = new ListLogger();
        var models = new ConcurrentDictionary<Type, EntityModel>();

        models[typeof(Source)] = new EntityModel
        {
            EntityType = typeof(Source),
            TopicName = "src",
            KeyProperties = new[] { typeof(Source).GetProperty(nameof(Source.Id))! },
            AllProperties = typeof(Source).GetProperties()
        };

        var targetModel = new EntityModel
        {
            EntityType = typeof(Target),
            TopicName = "tgt",
            QueryModel = new KsqlQueryRoot().From<Source>().Select(s => new { s.Id }).Build(),
            KeyProperties = new[] { typeof(Target).GetProperty(nameof(Target.Id))! },
            AllProperties = typeof(Target).GetProperties(),
            KeySchemaFullName = "k",
            ValueSchemaFullName = "v"
        };
        targetModel.SetStreamTableType(StreamTableType.Table);
        models[typeof(Target)] = targetModel;

        var ctx = CreateContext(client, logger, models);

        using (Kafka.Ksql.Linq.Core.Modeling.ModelCreatingScope.Enter())
        {
            var task = (Task)PrivateAccessor.InvokePrivate(ctx, "EnsureQueryEntityDdlAsync", new[] { typeof(Type), typeof(EntityModel) }, args: new object[] { typeof(Target), targetModel })!;
            await task;
        }

        var ddlMsgs = logger.Messages.Where(m => !m.StartsWith("ksql execute:", StringComparison.OrdinalIgnoreCase)).ToList();
        Assert.Equal(2, ddlMsgs.Count);
        Assert.Contains("CREATE TABLE", ddlMsgs[0]);
        Assert.DoesNotContain("CREATE TABLE AS", ddlMsgs[0]);
        Assert.Contains("INSERT INTO", ddlMsgs[1]);
        Assert.Equal(2, client.Statements.Count);
        Assert.Contains("CREATE TABLE", client.Statements[0]);

        Assert.Contains("INSERT INTO", client.Statements[1]);
        Assert.Contains("EMIT CHANGES", client.Statements[1]);
    }

    [Fact]
    public async Task LogsCtasForTable()
    {
        var client = new CapturingClient();
        var logger = new ListLogger();
        var models = new ConcurrentDictionary<Type, EntityModel>();

        models[typeof(Source)] = new EntityModel
        {
            EntityType = typeof(Source),
            TopicName = "src",
            KeyProperties = new[] { typeof(Source).GetProperty(nameof(Source.Id))! },
            AllProperties = typeof(Source).GetProperties()
        };

        var targetModel = new EntityModel
        {
            EntityType = typeof(Target),
            TopicName = "tgt",
            QueryModel = new KsqlQueryRoot().From<Source>().GroupBy(s => s.Id).Select(g => new { Id = g.Key, Count = g.Count() }).Build(),
            KeyProperties = new[] { typeof(Target).GetProperty(nameof(Target.Id))! },
            AllProperties = typeof(Target).GetProperties(),
            KeySchemaFullName = "k",
            ValueSchemaFullName = "v",
            Partitions = 1
        };
        targetModel.SetStreamTableType(StreamTableType.Table);
        models[typeof(Target)] = targetModel;

        var ctx = CreateContext(client, logger, models);

        using (Kafka.Ksql.Linq.Core.Modeling.ModelCreatingScope.Enter())
        {
            var task = (Task)PrivateAccessor.InvokePrivate(ctx, "EnsureQueryEntityDdlAsync", new[] { typeof(Type), typeof(EntityModel) }, args: new object[] { typeof(Target), targetModel })!;
            await task;
        }

        var ddlMsgs = logger.Messages.Where(m => !m.StartsWith("ksql execute:", StringComparison.OrdinalIgnoreCase)).ToList();
        Assert.Single(ddlMsgs);
        Assert.Contains("CREATE TABLE", ddlMsgs[0]);
        Assert.Contains("AS", ddlMsgs[0]);
        Assert.Equal(3, client.Statements.Count);
        Assert.Contains("CREATE TABLE", client.Statements[0]);
        Assert.Contains("SHOW QUERIES", client.Statements[1]);
        Assert.Contains("SHOW TOPICS", client.Statements[2]);
        Assert.DoesNotContain("INSERT INTO", string.Join("\n", client.Statements));

        DecimalPrecisionConfig.Configure(18, 2, null);
    }
}
