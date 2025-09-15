using Avro.Generic;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Models;
using Kafka.Ksql.Linq.Mapping;
using Kafka.Ksql.Linq.Query.Adapters;
using Kafka.Ksql.Linq.Query.Analysis;
using System.Linq;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Mapping;

public class MappingRegistryTests
{
    private class Sample
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private class Empty { }

    [Fact]
    public void Register_CreatesTypesWithExpectedNames()
    {
        var registry = new MappingRegistry();
        var keyProps = new[] { PropertyMeta.FromProperty(typeof(Sample).GetProperty(nameof(Sample.Id))!) };
        var valueProps = typeof(Sample).GetProperties()
            .Select(p => PropertyMeta.FromProperty(p))
            .ToArray();

        var mapping = registry.Register(
            typeof(Sample),
            keyProps,
            valueProps);

        Assert.Equal("sample_key", mapping.KeyType.Name);
        Assert.Equal("sample_value", mapping.ValueType.Name);
        Assert.Equal("kafka_ksql_linq_tests_mapping", mapping.KeyType.Namespace);
        Assert.Equal("kafka_ksql_linq_tests_mapping", mapping.ValueType.Namespace);
        var retrieved = registry.GetMapping(typeof(Sample));
        Assert.Same(mapping, retrieved);
    }

    [Fact]
    public void Register_EmptyProperties_AllowsZeroFieldMapping()
    {
        var registry = new MappingRegistry();
        var mapping = registry.Register(
            typeof(Empty),
            System.Array.Empty<PropertyMeta>(),
            System.Array.Empty<PropertyMeta>());
        Assert.Empty(mapping.KeyProperties);
        Assert.Empty(mapping.ValueProperties);
        Assert.Null(mapping.AvroKeyType);
        Assert.Null(mapping.AvroKeySchema);
    }

    [Fact]
    public void Register_GenericKey_SkipsAvroGeneration()
    {
        var registry = new MappingRegistry();
        var keyProps = new[] { PropertyMeta.FromProperty(typeof(Sample).GetProperty(nameof(Sample.Id))!) };
        var valueProps = typeof(Sample).GetProperties()
            .Select(p => PropertyMeta.FromProperty(p))
            .ToArray();

        var mapping = registry.Register(
            typeof(Sample),
            keyProps,
            valueProps,
            genericKey: true);

        Assert.Null(mapping.AvroKeyType);
        Assert.Null(mapping.AvroKeySchema);
        Assert.NotNull(mapping.AvroValueType);
    }

    [Fact]
    public void Register_GenericValue_UsesGenericRecord()
    {
        var registry = new MappingRegistry();
        var keyProps = new[] { PropertyMeta.FromProperty(typeof(Sample).GetProperty(nameof(Sample.Id))!) };
        var valueProps = typeof(Sample).GetProperties()
            .Select(p => PropertyMeta.FromProperty(p))
            .ToArray();

        var mapping = registry.Register(
            typeof(Sample),
            keyProps,
            valueProps,
            genericValue: true);

        Assert.Equal(typeof(GenericRecord), mapping.AvroValueType);
        Assert.NotNull(mapping.AvroValueSchema);
    }

    [Fact]
    public void RegisterEntityModel_OverrideNamespace_UsesOverride()
    {
        var model = new EntityModel
        {
            EntityType = typeof(object),
            TopicName = "sample",
            KeyProperties = new[] { typeof(Sample).GetProperty(nameof(Sample.Id))! },
            AllProperties = typeof(Sample).GetProperties()
        };

        var r1 = new MappingRegistry();
        var m1 = r1.RegisterEntityModel(model);
        Assert.Equal("system", m1.KeyType.Namespace);

        var r2 = new MappingRegistry();
        var m2 = r2.RegisterEntityModel(model, overrideNamespace: "custom_ns");
        Assert.Equal("custom_ns", m2.KeyType.Namespace);
        Assert.Equal("custom_ns", m2.ValueType.Namespace);
    }

    [Fact]
    public void RegisterDerivedModel_UsesSanitizedNamespace()
    {
        var derived = new DerivedEntity
        {
            Id = "bar_5m_live",
            Role = Role.Live,
            Timeframe = new Timeframe(5, "m"),
            KeyShape = new[] { new ColumnShape("Id", typeof(int), false) },
            ValueShape = new[] { new ColumnShape("Name", typeof(string), true) },
            BasedOnSpec = new BasedOnSpec(new[] { "Id" }, string.Empty, string.Empty, string.Empty)
        };
        var model = EntityModelAdapter.Adapt(new[] { derived })[0];
        model.EntityType = typeof(Sample);
        model.TopicName = derived.Id;
        model.KeyProperties = new[] { typeof(Sample).GetProperty(nameof(Sample.Id))! };
        model.AllProperties = typeof(Sample).GetProperties();

        Assert.Equal("bar_ksql", model.AdditionalSettings["namespace"]);

        var registry = new MappingRegistry();
        var mapping = registry.RegisterEntityModel(
            model,
            genericValue: true,
            overrideNamespace: model.AdditionalSettings["namespace"]?.ToString());

        Assert.Equal("bar_ksql", mapping.KeyType.Namespace);
        Assert.Equal("bar_ksql", mapping.ValueType.Namespace);
    }
}
