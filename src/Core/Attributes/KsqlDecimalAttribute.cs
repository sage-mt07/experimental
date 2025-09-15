using System;

namespace Kafka.Ksql.Linq.Core.Attributes;

[AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
public sealed class KsqlDecimalAttribute : Attribute
{
    public int Precision { get; }
    public int Scale { get; }

    public KsqlDecimalAttribute(int precision, int scale)
    {
        Precision = precision;
        Scale = scale;
    }
}
