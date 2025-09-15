namespace Kafka.Ksql.Linq.Messaging;

/// <summary>
/// Metadata derived from a Kafka message.
/// </summary>
public readonly record struct MessageMeta(
    string Topic,
    int Partition,
    long Offset,
    System.DateTimeOffset TimestampUtc,
    int? SchemaIdKey,
    int? SchemaIdValue,
    bool KeyIsNull,
    System.Collections.Generic.IReadOnlyDictionary<string, string> HeaderAllowList
);
