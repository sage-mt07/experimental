namespace Kafka.Ksql.Linq.Core.Dlq;

public interface IRateLimiter
{
    bool TryAcquire(int permits);
}

