using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Messaging.Consumers;
using Kafka.Ksql.Linq.Messaging.Producers;
using Kafka.Ksql.Linq.Messaging;
using Kafka.Ksql.Linq.Core.Dlq;
using Moq;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Messaging;

public class KafkaConsumerMappingErrorTests
{
    [Fact]
    public async Task HandleMappingException_SendsToDlq_And_Commits()
    {
        // Arrange: a minimal consume result
        var result = new ConsumeResult<Ignore, Ignore>
        {
            Topic = "test-topic",
            Partition = new Partition(0),
            Offset = new Offset(42),
            Message = new Message<Ignore, Ignore>
            {
                Timestamp = new Timestamp(DateTime.UtcNow),
                Headers = new Headers()
            }
        };

        var ex = new InvalidOperationException("mapping failed");

        var dlqProd = new Mock<IDlqProducer>(MockBehavior.Strict);
        dlqProd
            .Setup(p => p.ProduceAsync(It.IsAny<DlqEnvelope>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask)
            .Verifiable();

        var consumer = new Mock<IConsumer<Ignore, Ignore>>(MockBehavior.Strict);
        consumer.Setup(c => c.Commit(It.IsAny<ConsumeResult<Ignore, Ignore>>()))
                .Verifiable();

        var options = new DlqOptions
        {
            EnableForDeserializationError = true,
            ApplicationId = "ut-app",
            ConsumerGroup = "ut-group",
            Host = "ut-host"
        };

        var limiter = new SimpleRateLimiter(1000);

        // Act
        await KafkaConsumerManager.HandleMappingException(
            result,
            ex,
            dlqProd.Object,
            consumer.Object,
            options,
            limiter,
            CancellationToken.None);

        // Assert: DLQ sent and commit called
        dlqProd.Verify(p => p.ProduceAsync(It.IsAny<DlqEnvelope>(), It.IsAny<CancellationToken>()), Times.Once);
        consumer.Verify(c => c.Commit(It.IsAny<ConsumeResult<Ignore, Ignore>>()), Times.Once);
    }
}
