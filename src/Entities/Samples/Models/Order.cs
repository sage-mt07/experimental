using Kafka.Ksql.Linq.Core.Attributes;

namespace Kafka.Ksql.Linq.Entities.Samples.Models;

[KsqlTopic("orders", PartitionCount = 12, ReplicationFactor = 3)]
public class Order
{
    [KsqlKey(Order = 0)]
    public int OrderId { get; set; }

    [KsqlKey(Order = 1)]
    public int UserId { get; set; }

    public int ProductId { get; set; }

    public int Quantity { get; set; }
}
