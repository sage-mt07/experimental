namespace Kafka.Ksql.Linq.Messaging.Producers;

using System.Threading;
using System.Threading.Tasks;

public interface IDlqProducer
{
    Task ProduceAsync(DlqEnvelope envelope, CancellationToken cancellationToken);
}
