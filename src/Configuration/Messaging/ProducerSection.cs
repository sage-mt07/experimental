using System.Collections.Generic;
using System.ComponentModel;

namespace Kafka.Ksql.Linq.Configuration.Messaging;
public class ProducerSection
{
    [DefaultValue("All")]
    public string Acks { get; init; } = string.Empty;

    [DefaultValue("Snappy")]
    public string CompressionType { get; init; } = string.Empty;

    [DefaultValue(true)]
    public bool EnableIdempotence { get; init; }

    [DefaultValue(1)]
    public int MaxInFlightRequestsPerConnection { get; init; }

    [DefaultValue(5)]
    public int LingerMs { get; init; }

    [DefaultValue(16384)]
    public int BatchSize { get; init; }

    /// <summary>
    /// Maximum number of messages batched in one MessageSet.
    /// Set to 1 to flush each record (with LingerMs=0).
    /// </summary>
    [DefaultValue(10000)]
    public int BatchNumMessages { get; init; }

    [DefaultValue(120000)]
    public int DeliveryTimeoutMs { get; init; }

    [DefaultValue(100)]
    public int RetryBackoffMs { get; init; }

    [DefaultValue(int.MaxValue)]
    public int Retries { get; init; }

    [DefaultValue(33554432L)]
    public long BufferMemory { get; init; }

    public string? Partitioner { get; init; }

    [DefaultValue(typeof(Dictionary<string, string>))]
    public Dictionary<string, string> AdditionalProperties { get; init; } = new();
}
