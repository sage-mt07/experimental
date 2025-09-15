using Confluent.Kafka;
using System;
namespace Kafka.Ksql.Linq.Configuration.Abstractions;
public class KafkaSubscriptionOptions
{
    /// <summary>
    /// Consumer group ID
    /// </summary>
    public string? GroupId { get; set; }

    /// <summary>
    /// Enable auto commit
    /// </summary>
    public bool? AutoCommit { get; set; }

    /// <summary>
    /// Auto offset reset (uses Confluent.Kafka)
    /// </summary>
    public AutoOffsetReset? AutoOffsetReset { get; set; }

    /// <summary>
    /// Enable partition EOF
    /// </summary>
    public bool EnablePartitionEof { get; set; } = false;

    /// <summary>
    /// Session timeout
    /// </summary>
    public TimeSpan? SessionTimeout { get; set; }

    /// <summary>
    /// Heartbeat interval
    /// </summary>
    public TimeSpan? HeartbeatInterval { get; set; }

    /// <summary>
    /// Stop on error
    /// </summary>
    public bool StopOnError { get; set; } = false;

    /// <summary>
    /// Maximum number of records to poll
    /// </summary>
    public int? MaxPollRecords { get; set; }

    /// <summary>
    /// Maximum polling interval
    /// </summary>
    public TimeSpan? MaxPollInterval { get; set; }
}
