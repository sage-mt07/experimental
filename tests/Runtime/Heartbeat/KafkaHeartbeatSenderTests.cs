using Confluent.Kafka;
using Kafka.Ksql.Linq.Runtime.Heartbeat;
using Moq;
using System;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Runtime.Heartbeat;

public class KafkaHeartbeatSenderTests
{
    [Fact]
    public async Task TrySend_NoOp_WhenFlagFalse()
    {
        var flag = new LeadershipFlag();
        var producer = new Mock<IProducer<byte[], byte[]>>();
        var sender = new KafkaHeartbeatSender(producer.Object, flag, "hb_1m");
        await sender.TrySendAsync(new[] { "b", "s" }, DateTime.UtcNow, CancellationToken.None);
        producer.Verify(p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<byte[], byte[]>>(), It.IsAny<CancellationToken>()), Times.Never);
    }
}
