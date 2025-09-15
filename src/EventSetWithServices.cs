using Kafka.Ksql.Linq.Core.Abstractions;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Kafka.Ksql.Linq;

internal class EventSetWithServices<T> : EventSet<T> where T : class
{
    private readonly KsqlContext _ksqlContext;

    public EventSetWithServices(KsqlContext context, EntityModel entityModel)
        : base(context, entityModel, null, context.GetDlqProducer(), context.GetCommitManager())
    {
        _ksqlContext = context ?? throw new ArgumentNullException(nameof(context));
    }

    protected override async Task SendEntityAsync(T entity, Dictionary<string, string>? headers, CancellationToken cancellationToken)
    {
        var producerManager = _ksqlContext.GetProducerManager();
        var topic = GetTopicName();
        await producerManager.SendAsync(topic, entity, headers, cancellationToken);
    }

    public override async IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        var consumerManager = _ksqlContext.GetConsumerManager();
        await foreach (var (entity, _, _) in consumerManager.ConsumeAsync<T>(cancellationToken: cancellationToken))
        {
            yield return entity;
        }
    }
}
