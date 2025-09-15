using Kafka.Ksql.Linq.Core.Attributes;

namespace Kafka.Ksql.Linq.Entities.Samples.Models;

[KsqlTopic("products")]
public class Product
{
    [KsqlKey(Order = 0)]
    public int ProductId { get; set; }
    public string Name { get; set; } = string.Empty;
    public decimal Price { get; set; }
}
