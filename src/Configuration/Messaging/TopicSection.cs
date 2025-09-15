using System.Collections.Generic;
using System.ComponentModel;

namespace Kafka.Ksql.Linq.Configuration.Messaging;

/// <summary>
/// Topic-specific configuration (for both Producer and Consumer)
/// </summary>
public class TopicSection
{
    public string? TopicName { get; init; }

    [DefaultValue(typeof(ProducerSection))]
    public ProducerSection Producer { get; init; } = new();

    [DefaultValue(typeof(ConsumerSection))]
    public ConsumerSection Consumer { get; init; } = new();

    [DefaultValue(typeof(TopicCreationSection))]
    public TopicCreationSection? Creation { get; init; } = new();
}

/// <summary>
/// Topic creation settings
/// </summary>
public class TopicCreationSection
{
    [DefaultValue(1)]
    public int NumPartitions { get; init; }

    [DefaultValue((short)1)]
    public short ReplicationFactor { get; init; }

    [DefaultValue(typeof(Dictionary<string, string>))]
    public Dictionary<string, string> Configs { get; init; } = new();

    [DefaultValue(false)]
    public bool EnableAutoCreation { get; init; }
}