using System.Collections.Generic;

namespace Kafka.Ksql.Linq.Core.Abstractions;


public class KafkaMessageContext
{
    public string? MessageId { get; set; }
    public string? CorrelationId { get; set; }
    public int? TargetPartition { get; set; }
    public Dictionary<string, object> Headers { get; set; } = new();
    public Dictionary<string, object> Tags { get; set; } = new();


}
