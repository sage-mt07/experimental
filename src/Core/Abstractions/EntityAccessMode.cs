namespace Kafka.Ksql.Linq.Core.Abstractions;

/// <summary>
/// Specifies whether an entity is used for reads, writes, or both.
/// </summary>
public enum EntityAccessMode
{
    /// <summary>
    /// Entity is used for both read and write operations.
    /// </summary>
    ReadWrite,

    /// <summary>
    /// Entity is only used for reading from Kafka topics.
    /// </summary>
    ReadOnly,

    /// <summary>
    /// Entity is only used for writing to Kafka topics.
    /// </summary>
    WriteOnly
}

