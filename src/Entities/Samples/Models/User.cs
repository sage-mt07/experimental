using Kafka.Ksql.Linq.Core.Attributes;

namespace Kafka.Ksql.Linq.Entities.Samples.Models;

[KsqlTopic("users")]
public class User
{
    [KsqlKey(Order = 0)]
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
}
