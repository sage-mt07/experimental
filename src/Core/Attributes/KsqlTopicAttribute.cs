using System;

namespace Kafka.Ksql.Linq.Core.Attributes;

[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = true)]
public sealed class KsqlTopicAttribute : Attribute
{
    public string Name { get; }
    public int PartitionCount { get; set; } = 1;
    public short ReplicationFactor { get; set; } = 1;

    public KsqlTopicAttribute(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Topic name cannot be null or empty", nameof(name));
        Name = name;
    }
}
