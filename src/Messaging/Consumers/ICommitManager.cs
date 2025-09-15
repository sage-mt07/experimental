using System;

namespace Kafka.Ksql.Linq.Messaging.Consumers;

public interface ICommitManager
{
    void Bind(Type pocoType, string topic, object consumer);
    void Commit(object entity);
}
