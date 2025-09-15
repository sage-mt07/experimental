using Avro;
using Avro.Specific;
using Kafka.Ksql.Linq.Core.Models;
using Kafka.Ksql.Linq.Mapping;
using System;
using Xunit;

#nullable enable

namespace Kafka.Ksql.Linq.Tests.Mapping;

public class AvroCombineTests
{
    private class Poco
    {
        public int Id { get; set; }
        public string? Name { get; set; }
        public DateTime? CreatedAt { get; set; }
        public decimal Price { get; set; }
    }

    private class AvroValue : ISpecificRecord
    {
        public static Schema _SCHEMA = Schema.Parse(@"{
            'type':'record',
            'name':'AvroValue',
            'fields':[
                {'name':'id','type':'int','aliases':['Id']},
                {'name':'fullName','type':['null','string'],'default':null,'aliases':['Name']},
                {'name':'createdAt','type':['null',{'type':'long','logicalType':'timestamp-millis'}],'default':null},
                {'name':'price','type':{'type':'bytes','logicalType':'decimal','precision':18,'scale':2}}
            ]
        }".Replace('\'', '"'));
        public Schema Schema => _SCHEMA;

        public int Id { get; set; }
        public string? FullName { get; set; }
        public long? CreatedAt { get; set; }
        public AvroDecimal Price { get; set; }

        public object Get(int fieldPos) => fieldPos switch
        {
            0 => Id,
            1 => (object?)FullName!,
            2 => (object?)CreatedAt!,
            3 => Price,
            _ => throw new AvroRuntimeException("bad index")
        };

        public void Put(int fieldPos, object fieldValue)
        {
            switch (fieldPos)
            {
                case 0: Id = (int)fieldValue; break;
                case 1: FullName = (string?)fieldValue; break;
                case 2: CreatedAt = (long?)fieldValue; break;
                case 3: Price = (AvroDecimal)fieldValue; break;
                default: throw new AvroRuntimeException("bad index");
            }
        }
    }

    [Fact]
    public void CombineFromAvro_MapsFieldsAndConvertsTypes()
    {
        var metas = new[]
        {
            PropertyMeta.FromProperty(typeof(Poco).GetProperty(nameof(Poco.Id))!, sourceName: "id"),
            PropertyMeta.FromProperty(typeof(Poco).GetProperty(nameof(Poco.Name))!),
            PropertyMeta.FromProperty(typeof(Poco).GetProperty(nameof(Poco.CreatedAt))!),
            PropertyMeta.FromProperty(typeof(Poco).GetProperty(nameof(Poco.Price))!)
        };
        var mapping = new KeyValueTypeMapping { ValueProperties = metas };
        var av = new AvroValue { Id = 1, FullName = null, CreatedAt = 1000, Price = new AvroDecimal(decimal.Round(12.34m, 2)) };
        var poco = (Poco)mapping.CombineFromAvroKeyValue(null, av, typeof(Poco));
        Assert.Equal(1, poco.Id);
        Assert.Null(poco.Name);
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1000).UtcDateTime, poco.CreatedAt);
        Assert.Equal(12.34m, poco.Price);
    }

    [Fact]
    public void CombineFromAvro_MissingFieldThrows()
    {
        var metas = new[]
        {
            PropertyMeta.FromProperty(typeof(Poco).GetProperty(nameof(Poco.Id))!, sourceName: "id")
        };
        var mapping = new KeyValueTypeMapping { ValueProperties = metas };
        var av = new AvroValueMissing();
        Assert.Throws<InvalidOperationException>(() => mapping.CombineFromAvroKeyValue(null, av, typeof(Poco)));
    }
}

internal class AvroValueMissing : ISpecificRecord
{
    public static Schema _SCHEMA = Schema.Parse(@"{
        'type':'record',
        'name':'AvroValueMissing',
        'fields':[{'name':'name','type':'string'}]
    }".Replace('\'', '"'));
    public Schema Schema => _SCHEMA;
    public string Name { get; set; } = string.Empty;
    public object Get(int fieldPos) => fieldPos switch { 0 => Name, _ => throw new AvroRuntimeException("bad index") };
    public void Put(int fieldPos, object fieldValue)
    {
        switch (fieldPos)
        {
            case 0: Name = (string)fieldValue; break;
            default: throw new AvroRuntimeException("bad index");
        }
    }
}

