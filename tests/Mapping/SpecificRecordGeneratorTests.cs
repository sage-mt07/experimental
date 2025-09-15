using Avro;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Mapping;
using System;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Mapping;

public class SpecificRecordGeneratorTests
{
    private class OuterA
    {
        public class Record
        {
            public int Id { get; set; }
        }
    }

    private class OuterB
    {
        public class Record
        {
            public int Id { get; set; }
        }
    }

    [Fact]
    public void Generate_NestedTypesWithSameName_AreUnique()
    {
        var typeA = SpecificRecordGenerator.Generate(typeof(OuterA.Record));
        var typeB = SpecificRecordGenerator.Generate(typeof(OuterB.Record));

        Assert.NotEqual(typeA.FullName, typeB.FullName);
    }

    private class EmptyRecord
    {
    }

    [Fact]
    public void Generate_ClassWithNoProperties_ProducesSchemaWithoutFields()
    {
        var type = SpecificRecordGenerator.Generate(typeof(EmptyRecord));
        var instance = (Avro.Specific.ISpecificRecord)Activator.CreateInstance(type)!;
        var recordSchema = (Avro.RecordSchema)instance.Schema;
        Assert.Empty(recordSchema.Fields);
    }

    private class DictionaryRecord
    {
        public Dictionary<string, string> Headers { get; set; } = new();
    }

    [Fact]
    public void Generate_DictionaryProperty_ProducesMapSchema()
    {
        var type = SpecificRecordGenerator.Generate(typeof(DictionaryRecord));
        var instance = (Avro.Specific.ISpecificRecord)Activator.CreateInstance(type)!;
        var schema = (RecordSchema)instance.Schema;
        var field = Assert.Single(schema.Fields);
        var mapSchema = Assert.IsType<MapSchema>(field.Schema);
        Assert.Equal(Schema.Type.String, ((PrimitiveSchema)mapSchema.ValueSchema).Tag);
        var defaultMap = Assert.IsType<Newtonsoft.Json.Linq.JObject>(field.DefaultValue);
        Assert.False(defaultMap.HasValues);
    }

    private class BadKeyRecord
    {
        public Dictionary<int, string> Headers { get; set; } = new();
    }

    private class BadValueRecord
    {
        public Dictionary<string, int> Headers { get; set; } = new();
    }

    [Fact]
    public void Generate_NonStringDictionary_Throws()
    {
        Assert.Throws<NotSupportedException>(() => SpecificRecordGenerator.Generate(typeof(BadKeyRecord)));
        Assert.Throws<NotSupportedException>(() => SpecificRecordGenerator.Generate(typeof(BadValueRecord)));
    }

    private class DecimalRecord
    {
        public decimal Amount { get; set; }
    }

    private class AttributeDecimalRecord
    {
        [KsqlDecimal(18, 4)]
        public decimal Amount { get; set; }
    }

    [Fact]
    public void Generate_DecimalProperty_RespectsPrecisionConfig()
    {
        var originalPrecision = Kafka.Ksql.Linq.Configuration.DecimalPrecisionConfig.DecimalPrecision;
        var originalScale = Kafka.Ksql.Linq.Configuration.DecimalPrecisionConfig.DecimalScale;
        try
        {
            Kafka.Ksql.Linq.Configuration.DecimalPrecisionConfig.Configure(10, 3, null);
            var type = SpecificRecordGenerator.Generate(typeof(DecimalRecord));
            var instance = (Avro.Specific.ISpecificRecord)Activator.CreateInstance(type)!;
            var schema = (RecordSchema)instance.Schema;
            var field = Assert.Single(schema.Fields);
            var schemaJson = field.Schema.ToString();
            Assert.Contains("\"logicalType\":\"decimal\"", schemaJson);
            Assert.Contains("\"precision\":10", schemaJson);
            Assert.Contains("\"scale\":3", schemaJson);
            Assert.Equal(typeof(Avro.AvroDecimal), type.GetProperty(nameof(DecimalRecord.Amount))!.PropertyType);
        }
        finally
        {
            Kafka.Ksql.Linq.Configuration.DecimalPrecisionConfig.Configure(originalPrecision, originalScale, null);
        }
    }

    [Fact]
    public void Generate_DecimalProperty_WithAttribute_OverridesConfig()
    {
        var originalPrecision = Kafka.Ksql.Linq.Configuration.DecimalPrecisionConfig.DecimalPrecision;
        var originalScale = Kafka.Ksql.Linq.Configuration.DecimalPrecisionConfig.DecimalScale;
        try
        {
            Kafka.Ksql.Linq.Configuration.DecimalPrecisionConfig.Configure(8, 2, null);
            var type = SpecificRecordGenerator.Generate(typeof(AttributeDecimalRecord));
            var instance = (Avro.Specific.ISpecificRecord)Activator.CreateInstance(type)!;
            var schema = (RecordSchema)instance.Schema;
            var field = Assert.Single(schema.Fields);
            var schemaJson = field.Schema.ToString();
            Assert.Contains("\"precision\":18", schemaJson);
            Assert.Contains("\"scale\":4", schemaJson);
        }
        finally
        {
            Kafka.Ksql.Linq.Configuration.DecimalPrecisionConfig.Configure(originalPrecision, originalScale, null);
        }
    }
}
