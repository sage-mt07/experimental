using Avro;
using Avro.Specific;

namespace Kafka.Ksql.Linq.Tests.Integration.Streamiz.Models;

public class Address : ISpecificRecord
{
    public static Schema _SCHEMA = Schema.Parse("{\"type\":\"record\",\"name\":\"Address\",\"namespace\":\"Streamiz.Tests\",\"fields\":[{\"name\":\"street\",\"type\":\"string\"},{\"name\":\"zip\",\"type\":\"int\"}]}");
    public Schema Schema => _SCHEMA;

    public string street { get; set; } = string.Empty;
    public int zip { get; set; }

    public object Get(int fieldPos) => fieldPos switch
    {
        0 => street,
        1 => zip,
        _ => throw new AvroRuntimeException("Bad index")
    };

    public void Put(int fieldPos, object value)
    {
        switch (fieldPos)
        {
            case 0:
                street = (string)value;
                break;
            case 1:
                zip = (int)value;
                break;
            default:
                throw new AvroRuntimeException("Bad index");
        }
    }
}
