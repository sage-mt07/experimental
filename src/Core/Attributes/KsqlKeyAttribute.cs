using System;

namespace Kafka.Ksql.Linq.Core.Attributes;

[AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
public sealed class KsqlKeyAttribute : Attribute
{
    public int Order { get; set; }

    public KsqlKeyAttribute(int order = 0)
    {
        Order = order;
    }
}
