using Confluent.Kafka;
using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Infrastructure.Admin;
using Kafka.Ksql.Linq.Infrastructure.KsqlDb;
using Kafka.Ksql.Linq.Mapping;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Xunit;

#nullable enable

namespace Kafka.Ksql.Linq.Tests.Core;

public class EnsureSimpleEntityDdlAsyncTests
{
    private class FailingClient : IKsqlDbClient
    {
        public Task<KsqlDbResponse> ExecuteStatementAsync(string statement) => Task.FromResult(new KsqlDbResponse(false, "err"));
        public Task<KsqlDbResponse> ExecuteExplainAsync(string ksql) => Task.FromResult(new KsqlDbResponse(true, ""));
        public Task<HashSet<string>> GetTableTopicsAsync() => Task.FromResult(new HashSet<string>());
        public Task<int> ExecuteQueryStreamCountAsync(string sql, TimeSpan? timeout = null) => Task.FromResult(0);
        public Task<int> ExecutePullQueryCountAsync(string sql, TimeSpan? timeout = null) => Task.FromResult(0);
    }

    private class DummyContext : KsqlContext
    {
        private DummyContext() : base(new Microsoft.Extensions.Configuration.ConfigurationBuilder().Build()) { }
    }

    private class FakeAdminClient : DispatchProxy
    {
        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            switch (targetMethod?.Name)
            {
                case "GetMetadata":
                    var meta = (Metadata)RuntimeHelpers.GetUninitializedObject(typeof(Metadata));
                    typeof(Metadata).GetProperty("Topics", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)!
                        .SetValue(meta, new List<TopicMetadata>());
                    return meta;
                case "CreateTopicsAsync":
                    return Task.CompletedTask;
                case "Dispose":
                    return null;
                case "get_Name":
                    return "fake";
                case "get_Handle":
                    return null!;
            }
            throw new NotImplementedException(targetMethod?.Name);
        }
    }

    private static DummyContext CreateContext()
    {
        var ctx = (DummyContext)RuntimeHelpers.GetUninitializedObject(typeof(DummyContext));
        var dsl = new KsqlDslOptions();
        DefaultValueBinder.ApplyDefaults(dsl);
        typeof(KsqlContext).GetField("_dslOptions", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(ctx, dsl);
        typeof(KsqlContext).GetField("_mappingRegistry", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(ctx, new MappingRegistry());
        typeof(KsqlContext).GetField("_entityModels", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(ctx, new ConcurrentDictionary<Type, EntityModel>());
        typeof(KsqlContext).GetField("_ksqlDbClient", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(ctx, new FailingClient());
        var admin = (KafkaAdminService)RuntimeHelpers.GetUninitializedObject(typeof(KafkaAdminService));
        var adminOpts = new KsqlDslOptions();
        DefaultValueBinder.ApplyDefaults(adminOpts);
        typeof(KafkaAdminService).GetField("_options", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(admin, adminOpts);
        var proxy = DispatchProxy.Create<IAdminClient, FakeAdminClient>();
        typeof(KafkaAdminService).GetField("_adminClient", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(admin, proxy);
        typeof(KsqlContext).GetField("_adminService", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(ctx, admin);
        typeof(KsqlContext).GetField("_logger", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(ctx, NullLogger.Instance);
        return ctx;
    }

    [Fact]
    public async Task ThrowsWhenKsqlExecutionFails()
    {
        var ctx = CreateContext();
        var model = new EntityModel
        {
            EntityType = typeof(TestEntity),
            TopicName = "dead-letter-queue",
            KeyProperties = new[] { typeof(TestEntity).GetProperty(nameof(TestEntity.Id))! },
            AllProperties = typeof(TestEntity).GetProperties()
        };

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            var task = (Task)PrivateAccessor.InvokePrivate(ctx, "EnsureSimpleEntityDdlAsync", new[] { typeof(Type), typeof(EntityModel) }, args: new object[] { typeof(TestEntity), model })!;
            await task;
        });
    }
}
