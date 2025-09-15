using Kafka.Ksql.Linq.Messaging.Producers;
using Kafka.Ksql.Linq.Core.Abstractions;

#nullable enable
using System.Reflection;

namespace Kafka.Ksql.Linq.Tests.Integration;

internal static class EventSetExtensions
{
    public static async Task AddAsync<T>(this EventSet<T> set, T entity, KafkaMessageContext context, CancellationToken cancellationToken = default) where T : class
    {
        if (set == null) throw new ArgumentNullException(nameof(set));
        if (entity == null) throw new ArgumentNullException(nameof(entity));
        var field = typeof(EventSet<T>).GetField("_context", BindingFlags.NonPublic | BindingFlags.Instance);
        var ksqlContext = (KsqlContext?)field!.GetValue(set);
        var manager = Kafka.Ksql.Linq.Tests.PrivateAccessor.InvokePrivate<KafkaProducerManager>(ksqlContext!, "GetProducerManager", Type.EmptyTypes);
        var producerTask = Kafka.Ksql.Linq.Tests.PrivateAccessor.InvokePrivate<Task<KafkaProducerManager.ProducerHolder>>(
            manager,
            "GetProducerAsync",
            new[] { typeof(string) },
            new[] { typeof(T) },
            Type.Missing);
        var producer = await producerTask;
        await producer.SendAsync(null, entity, context, cancellationToken);
    }
}
