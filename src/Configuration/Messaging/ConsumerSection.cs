using System.Collections.Generic;
using System.ComponentModel;

namespace Kafka.Ksql.Linq.Configuration.Messaging;

/// <summary>
/// Consumer configuration
/// </summary>
public class ConsumerSection
{
    [DefaultValue("default-group")]
    public string GroupId { get; init; } = string.Empty;

    [DefaultValue("Latest")]
    public string AutoOffsetReset { get; init; } = string.Empty;

    [DefaultValue(true)]
    public bool EnableAutoCommit { get; init; }

    [DefaultValue(5000)]
    public int AutoCommitIntervalMs { get; init; }

    [DefaultValue(30000)]
    public int SessionTimeoutMs { get; init; }

    [DefaultValue(3000)]
    public int HeartbeatIntervalMs { get; init; }

    [DefaultValue(300000)]
    public int MaxPollIntervalMs { get; init; }

    [DefaultValue(500)]
    public int MaxPollRecords { get; init; }

    [DefaultValue(1)]
    public int FetchMinBytes { get; init; }

    [DefaultValue(500)]
    public int FetchMaxWaitMs { get; init; }

    [DefaultValue(52428800)]
    public int FetchMaxBytes { get; init; }

    public string? PartitionAssignmentStrategy { get; init; }

    [DefaultValue("ReadUncommitted")]
    public string IsolationLevel { get; init; } = string.Empty;

    [DefaultValue(typeof(Dictionary<string, string>))]
    public Dictionary<string, string> AdditionalProperties { get; init; } = new();
}