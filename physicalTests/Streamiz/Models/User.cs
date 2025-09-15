using Avro;
using Avro.Specific;

namespace Kafka.Ksql.Linq.Tests.Integration.Streamiz.Models;

public class User : ISpecificRecord
{
    public static Schema _SCHEMA = Schema.Parse("{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"Streamiz.Tests\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}");
    public Schema Schema => _SCHEMA;

    public string name { get; set; } = string.Empty;
    public int age { get; set; }

    public object Get(int fieldPos) => fieldPos switch
    {
        0 => name,
        1 => age,
        _ => throw new AvroRuntimeException("Bad index")
    };

    public void Put(int fieldPos, object value)
    {
        switch (fieldPos)
        {
            case 0:
                name = (string)value;
                break;
            case 1:
                age = (int)value;
                break;
            default:
                throw new AvroRuntimeException("Bad index");
        }
    }
}
