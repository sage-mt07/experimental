using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Configuration;
using System;
using System.Net.Http;
using System.Reflection;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Application;

public class KsqlDbExecutionExtensionsTests
{
    private class DummyContext : KsqlContext
    {
        public DummyContext(KsqlDslOptions opt) : base(opt) { }
        protected override bool SkipSchemaRegistration => true;
        protected override void OnModelCreating(Kafka.Ksql.Linq.Core.Abstractions.IModelBuilder modelBuilder) { }
    }

    [Fact]
    public void CreateClient_UsesSchemaRegistryHost()
    {
        var ctx = new DummyContext(new KsqlDslOptions());
        var field = typeof(KsqlContext).GetField("_dslOptions", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var opts = (KsqlDslOptions)field.GetValue(ctx)!;
        var schema = opts.SchemaRegistry;
        typeof(SchemaRegistrySection).GetProperty("Url")!.SetValue(schema, "http://example.com:8085");

        var method = typeof(KsqlContext)
            .GetMethod("CreateClient", BindingFlags.NonPublic | BindingFlags.Instance)!;
        using var client = (HttpClient)method.Invoke(ctx, null)!;
        Assert.Equal(new Uri("http://example.com:8085"), client.BaseAddress);
    }

    [Fact]
    public void CreateClient_UsesDefaultWhenSchemaRegistryMissing()
    {
        var ctx = new DummyContext(new KsqlDslOptions());
        var field = typeof(KsqlContext).GetField("_dslOptions", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var opts = (KsqlDslOptions)field.GetValue(ctx)!;
        typeof(SchemaRegistrySection).GetProperty("Url")!.SetValue(opts.SchemaRegistry, "");
        typeof(CommonSection).GetProperty("BootstrapServers")!.SetValue(opts.Common, "example.com:9092");

        var method = typeof(KsqlContext)
            .GetMethod("CreateClient", BindingFlags.NonPublic | BindingFlags.Instance)!;
        using var client = (HttpClient)method.Invoke(ctx, null)!;
        Assert.Equal(new Uri("http://localhost:8088"), client.BaseAddress);
    }
}
