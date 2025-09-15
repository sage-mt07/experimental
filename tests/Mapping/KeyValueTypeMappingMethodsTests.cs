using Avro;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Core.Models;
using Kafka.Ksql.Linq.Mapping;
using System;
using System.Linq;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Mapping;

public class KeyValueTypeMappingMethodsTests
{
    private class Sample
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Fact]
    public void ExtractAndCombine_RoundTrip_ReturnsEquivalentObject()
    {
        var registry = new MappingRegistry();
        var keyProps = new[] { PropertyMeta.FromProperty(typeof(Sample).GetProperty(nameof(Sample.Id))!) };
        var valueProps = typeof(Sample).GetProperties()
            .Select(p => PropertyMeta.FromProperty(p))
            .ToArray();

        var mapping = registry.Register(typeof(Sample), keyProps, valueProps);

        var sample = new Sample { Id = 5, Name = "x" };

        var keyObj = mapping.ExtractKey(sample);
        var valueObj = mapping.ExtractValue(sample);

        var restored = (Sample)mapping.CombineFromKeyValue(keyObj, valueObj, typeof(Sample));

        Assert.Equal(sample.Id, restored.Id);
        Assert.Equal(sample.Name, restored.Name);
    }

    [Fact]
    public void PopulateKeyValue_CopiesValuesIntoProvidedInstances()
    {
        var registry = new MappingRegistry();
        var keyProps = new[] { PropertyMeta.FromProperty(typeof(Sample).GetProperty(nameof(Sample.Id))!) };
        var valueProps = typeof(Sample).GetProperties()
            .Select(p => PropertyMeta.FromProperty(p))
            .ToArray();

        var mapping = registry.Register(typeof(Sample), keyProps, valueProps);

        var sample = new Sample { Id = 10, Name = "abc" };
        var keyObj = System.Activator.CreateInstance(mapping.KeyType)!;
        var valueObj = System.Activator.CreateInstance(mapping.ValueType)!;

        mapping.PopulateKeyValue(sample, keyObj, valueObj);

        var restored = (Sample)mapping.CombineFromKeyValue(keyObj, valueObj, typeof(Sample));

        Assert.Equal(sample.Id, restored.Id);
        Assert.Equal(sample.Name, restored.Name);
    }

    private class DecimalSample
    {
        [KsqlDecimal(18, 4)]
        public decimal Price { get; set; }
    }

    [Fact]
    public void ExtractAvroValue_UsesAttributeScale()
    {
        var registry = new MappingRegistry();
        var valueProps = typeof(DecimalSample).GetProperties()
            .Select(p => PropertyMeta.FromProperty(p))
            .ToArray();
        var mapping = registry.Register(typeof(DecimalSample), Array.Empty<PropertyMeta>(), valueProps);
        var sample = new DecimalSample { Price = 12.345678m };
        var avro = mapping.ExtractAvroValue(sample);
        var avroProp = mapping.AvroValueType!.GetProperty(nameof(DecimalSample.Price))!;
        var avroDec = (AvroDecimal)avroProp.GetValue(avro)!;
        Assert.Equal(12.3457m, (decimal)avroDec);
    }

    private class GuidSample
    {
        public Guid Id { get; set; }
    }

    [Fact]
    public void Guid_IsConvertedToStringAndBack()
    {
        var registry = new MappingRegistry();
        var valueProps = typeof(GuidSample).GetProperties()
            .Select(p => PropertyMeta.FromProperty(p))
            .ToArray();
        var mapping = registry.Register(typeof(GuidSample), Array.Empty<PropertyMeta>(), valueProps);
        var sample = new GuidSample { Id = Guid.NewGuid() };

        var avro = mapping.ExtractAvroValue(sample);
        var avroProp = mapping.AvroValueType!.GetProperty(nameof(GuidSample.Id))!;
        Assert.Equal(typeof(string), avroProp.PropertyType);
        Assert.Equal(sample.Id.ToString("D"), avroProp.GetValue(avro));

        var restored = (GuidSample)mapping.CombineFromAvroKeyValue(null, avro, typeof(GuidSample));
        Assert.Equal(sample.Id, restored.Id);
    }
}
