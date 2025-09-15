namespace Kafka.Ksql.Linq.Core.Abstractions;

public enum DeserializationErrorPolicy
{
    Skip,
    Retry,
    DLQ
}
