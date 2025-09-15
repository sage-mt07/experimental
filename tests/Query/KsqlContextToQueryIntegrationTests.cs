using Confluent.SchemaRegistry;
using Kafka.Ksql.Linq;
using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Core.Extensions;
using Kafka.Ksql.Linq.Core.Modeling;
using Kafka.Ksql.Linq.Mapping;
using Kafka.Ksql.Linq.Query.Builders;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Xunit;

#nullable enable

namespace Kafka.Ksql.Linq.Tests.Query;

public class KsqlContextToQueryIntegrationTests
{
    private class Order
    {
        public int Id { get; set; }
    }

    private class ReorderedView
    {
        public string Name { get; set; } = string.Empty;
        [KsqlKey(Order = 0)]
        public int Id { get; set; }
    }

    private class InvalidOrderView
    {
        [KsqlKey(Order = 0)]
        public int Id { get; set; }
        [KsqlKey(Order = 1)]
        public int SubId { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private class KeylessView
    {
        public string Name { get; set; } = string.Empty;
    }
    private class OrderView
    {
        public int Id { get; set; }
    }

    private class StubSchemaRegistry : ISchemaRegistryClient
    {
        public List<string> Subjects { get; } = new();
        public Task<int> RegisterSchemaAsync(string subject, string schema, bool normalize) { Subjects.Add(subject); return Task.FromResult(1); }
        public Task<int> RegisterSchemaAsync(string subject, Schema schema, bool normalize) => RegisterSchemaAsync(subject, schema.SchemaString, normalize);
        public Task<int> RegisterSchemaAsync(string subject, string schema) => RegisterSchemaAsync(subject, schema, false);
        public Task<int> RegisterSchemaAsync(string subject, Schema schema) => RegisterSchemaAsync(subject, schema.SchemaString, false);
        public Task<RegisteredSchema> GetLatestSchemaAsync(string subject) => throw new NotImplementedException();
        public Task<RegisteredSchema> RegisterSchemaWithResponseAsync(string subject, Schema schema, bool normalize) => throw new NotImplementedException();
        public Task<int> GetSchemaIdAsync(string subject, string schema, bool normalize) => throw new NotImplementedException();
        public Task<int> GetSchemaIdAsync(string subject, Schema schema, bool normalize) => throw new NotImplementedException();
        public Task<Schema> GetSchemaAsync(int id, string format) => throw new NotImplementedException();
        public Task<Schema> GetSchemaBySubjectAndIdAsync(string subject, int id, string format) => throw new NotImplementedException();
        public Task<Schema> GetSchemaByGuidAsync(string id, string format) => throw new NotImplementedException();
        public Task<RegisteredSchema> LookupSchemaAsync(string subject, Schema schema, bool normalize, bool lookupDeletedSchema) => throw new NotImplementedException();
        public Task<RegisteredSchema> GetRegisteredSchemaAsync(string subject, int version, bool lookupDeletedSchema) => throw new NotImplementedException();
        public Task<string> GetSchemaAsync(string subject, int version) => throw new NotImplementedException();
        public Task<RegisteredSchema> GetLatestWithMetadataAsync(string subject, IDictionary<string, string> headers, bool ignoreDeletedSchemas) => throw new NotImplementedException();
        public Task<List<string>> GetAllSubjectsAsync() => Task.FromResult(Subjects);
        public Task<List<int>> GetSubjectVersionsAsync(string subject) => throw new NotImplementedException();
        public Task<bool> IsCompatibleAsync(string subject, string schema) => throw new NotImplementedException();
        public Task<bool> IsCompatibleAsync(string subject, Schema schema) => throw new NotImplementedException();
        public string ConstructKeySubjectName(string subject, string topic) => throw new NotImplementedException();
        public string ConstructValueSubjectName(string subject, string topic) => throw new NotImplementedException();
        public Task<Compatibility> GetCompatibilityAsync(string subject) => throw new NotImplementedException();
        public Task<Compatibility> UpdateCompatibilityAsync(Compatibility compatibility, string subject) => throw new NotImplementedException();
        public IEnumerable<KeyValuePair<string, string>> Config => Array.Empty<KeyValuePair<string, string>>();
        public IAuthenticationHeaderValueProvider? AuthHeaderProvider => null;
        public System.Net.IWebProxy? Proxy => null;
        public int MaxCachedSchemas => 1000;
        public void ClearCaches() { }
        public void ClearLatestCaches() { }
        public void Dispose() { }
    }

    private class TestContext : KsqlContext
    {
        public MappingRegistry Registry => (MappingRegistry)typeof(KsqlContext)
            .GetField("_mappingRegistry", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(this)!;

        public TestContext(ISchemaRegistryClient client) : base(new KsqlDslOptions())
        {
            typeof(KsqlContext).GetField("_schemaRegistryClient", BindingFlags.NonPublic | BindingFlags.Instance)!
                .SetValue(this, new Lazy<ISchemaRegistryClient>(() => client));
        }

        protected override bool SkipSchemaRegistration => true;

        protected override void OnModelCreating(IModelBuilder builder)
        {
            builder.Entity<Order>();
            builder.Entity<OrderView>().ToQuery(q => q.From<Order>()
                .Where(o => o.Id > 0)
                .Select(o => new OrderView { Id = o.Id }));
        }
    }

    private class ReorderedContext : KsqlContext
    {
        public MappingRegistry Registry => (MappingRegistry)typeof(KsqlContext)
            .GetField("_mappingRegistry", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(this)!;

        public ReorderedContext(ISchemaRegistryClient client) : base(new KsqlDslOptions())
        {
            typeof(KsqlContext).GetField("_schemaRegistryClient", BindingFlags.NonPublic | BindingFlags.Instance)!
                .SetValue(this, new Lazy<ISchemaRegistryClient>(() => client));
        }

        protected override bool SkipSchemaRegistration => true;

        protected override void OnModelCreating(IModelBuilder builder)
        {
            builder.Entity<Order>();
            builder.Entity<ReorderedView>().ToQuery(q => q.From<Order>()
                .Select(o => new ReorderedView { Name = "x", Id = o.Id }));
        }
    }

    private class KeylessContext : KsqlContext
    {
        public MappingRegistry Registry => (MappingRegistry)typeof(KsqlContext)
            .GetField("_mappingRegistry", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(this)!;

        public KeylessContext(ISchemaRegistryClient client) : base(new KsqlDslOptions())
        {
            typeof(KsqlContext).GetField("_schemaRegistryClient", BindingFlags.NonPublic | BindingFlags.Instance)!
                .SetValue(this, new Lazy<ISchemaRegistryClient>(() => client));
        }

        protected override bool SkipSchemaRegistration => true;

        protected override void OnModelCreating(IModelBuilder builder)
        {
            builder.Entity<Order>();
            builder.Entity<KeylessView>().ToQuery(q => q.From<Order>()
                .Select(o => new KeylessView { Name = "x" }));
        }
    }

    private class InvalidOrderContext : KsqlContext
    {
        public InvalidOrderContext(ISchemaRegistryClient client) : base(new KsqlDslOptions())
        {
            typeof(KsqlContext).GetField("_schemaRegistryClient", BindingFlags.NonPublic | BindingFlags.Instance)!
                .SetValue(this, new Lazy<ISchemaRegistryClient>(() => client));
        }

        protected override bool SkipSchemaRegistration => true;

        protected override void OnModelCreating(IModelBuilder builder)
        {
            builder.Entity<Order>();
            builder.Entity<InvalidOrderView>().ToQuery(q => q.From<Order>()
                .Select(o => new InvalidOrderView { SubId = o.Id, Id = o.Id, Name = "x" }));
        }
    }

    [Fact(Skip = "Requires full query pipeline")]
    public void ToQuery_Model_IsRegisteredAndSqlGenerated()
    {
        var client = new StubSchemaRegistry();
        using var ctx = new TestContext(client);

        var models = ctx.GetEntityModels();
        Assert.Contains(typeof(OrderView), models.Keys);
        var model = models[typeof(OrderView)];
        Assert.NotNull(model.QueryModel);

        var sql = KsqlCreateStatementBuilder.Build(model.GetTopicName(), model.QueryModel!);

        Assert.Equal(typeof(OrderView), ctx.Registry.GetLastRegistered());
    }

    [Fact]
    public void ToQuery_Retains_Model()
    {
        var client = new StubSchemaRegistry();
        using var ctx = new TestContext(client);

        var models = ctx.GetEntityModels();
        Assert.Contains(typeof(OrderView), models.Keys);
    }

    [Fact]
    public void ToQuery_SelectOrder_IsPreserved()
    {
        var client = new StubSchemaRegistry();
        using var ctx = new ReorderedContext(client);

        var mapping = ctx.Registry.GetMapping(typeof(ReorderedView));
        Assert.Equal(new[] { nameof(ReorderedView.Id) }, mapping.KeyProperties.Select(p => p.Name));
        Assert.Equal(new[] { nameof(ReorderedView.Name) }, mapping.ValueProperties.Select(p => p.Name));
    }

    [Fact]
    public void ToQuery_KeylessEntity_RegistersWithoutKeys()
    {
        var client = new StubSchemaRegistry();
        using var ctx = new KeylessContext(client);

        var mapping = ctx.Registry.GetMapping(typeof(KeylessView));
        Assert.Empty(mapping.KeyProperties);
        Assert.Single(mapping.ValueProperties);
    }

    [Fact]
    public void ToQuery_InvalidProjection_Throws()
    {
        var client = new StubSchemaRegistry();
        Assert.Throws<InvalidOperationException>(() => new InvalidOrderContext(client));
    }
}
