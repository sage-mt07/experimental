namespace Kafka.Ksql.Linq.Messaging.Producers;

using System.Threading;
using System.Threading.Tasks;

internal class DlqProducer : IDlqProducer
{
    private readonly KafkaProducerManager _manager;
    private readonly string _topic;

    public DlqProducer(KafkaProducerManager manager, string topic)
    {
        _manager = manager;
        _topic = topic;
    }

    public Task ProduceAsync(DlqEnvelope envelope, CancellationToken cancellationToken)
        => _manager.SendAsync(_topic, envelope, null, cancellationToken);
}
