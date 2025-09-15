using Confluent.Kafka;
using Kafka.Ksql.Linq.Messaging.Producers;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

#nullable enable

namespace Kafka.Ksql.Linq.Tests.Messaging.Producers.Simple;

public class SimpleKafkaProducerTests
{
    private class DummyKeylessProducer : IProducer<Null, string>
    {
        public List<(string Topic, Message<Null, string> Message)> Produced { get; } = new();
        public string Name => "dummy";
        public Handle Handle => throw new NotImplementedException();
        public int AddBrokers(string brokers) => 0;
        public void SetSaslCredentials(string username, string password) { }
        public void Dispose() { }
        public void Flush(CancellationToken cancellationToken = default) { }
        public int Flush(TimeSpan timeout) => 0;
        public void Produce(string topic, Message<Null, string> message, Action<DeliveryReport<Null, string>>? handler = null) => throw new NotImplementedException();
        public void Produce(TopicPartition topicPartition, Message<Null, string> message, Action<DeliveryReport<Null, string>>? handler = null) => throw new NotImplementedException();
        public int Poll(TimeSpan timeout) => 0;
        public Task<DeliveryResult<Null, string>> ProduceAsync(string topic, Message<Null, string> message, CancellationToken cancellationToken = default)
        {
            Produced.Add((topic, message));
            return Task.FromResult(new DeliveryResult<Null, string> { Topic = topic, Message = message });
        }
        public Task<DeliveryResult<Null, string>> ProduceAsync(TopicPartition topicPartition, Message<Null, string> message, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public void InitTransactions(TimeSpan timeout) { }
        public void BeginTransaction() { }
        public void CommitTransaction(TimeSpan timeout) { }
        public void CommitTransaction() { }
        public void AbortTransaction(TimeSpan timeout) { }
        public void AbortTransaction() { }
        public void SendOffsetsToTransaction(IEnumerable<TopicPartitionOffset> offsets, IConsumerGroupMetadata groupMetadata, TimeSpan timeout) { }
    }

    private class DummyKeyedProducer : IProducer<string, string>
    {
        public List<(string Topic, Message<string, string> Message)> Produced { get; } = new();
        public string Name => "dummy";
        public Handle Handle => throw new NotImplementedException();
        public int AddBrokers(string brokers) => 0;
        public void SetSaslCredentials(string username, string password) { }
        public void Dispose() { }
        public void Flush(CancellationToken cancellationToken = default) { }
        public int Flush(TimeSpan timeout) => 0;
        public void Produce(string topic, Message<string, string> message, Action<DeliveryReport<string, string>>? handler = null) => throw new NotImplementedException();
        public void Produce(TopicPartition topicPartition, Message<string, string> message, Action<DeliveryReport<string, string>>? handler = null) => throw new NotImplementedException();
        public int Poll(TimeSpan timeout) => 0;
        public Task<DeliveryResult<string, string>> ProduceAsync(string topic, Message<string, string> message, CancellationToken cancellationToken = default)
        {
            Produced.Add((topic, message));
            return Task.FromResult(new DeliveryResult<string, string> { Topic = topic, Message = message });
        }
        public Task<DeliveryResult<string, string>> ProduceAsync(TopicPartition topicPartition, Message<string, string> message, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public void InitTransactions(TimeSpan timeout) { }
        public void BeginTransaction() { }
        public void CommitTransaction(TimeSpan timeout) { }
        public void CommitTransaction() { }
        public void AbortTransaction(TimeSpan timeout) { }
        public void AbortTransaction() { }
        public void SendOffsetsToTransaction(IEnumerable<TopicPartitionOffset> offsets, IConsumerGroupMetadata groupMetadata, TimeSpan timeout) { }
    }

    [Fact]
    public async Task SendAsync_NoKey_SendsMessage()
    {
        var producer = new DummyKeylessProducer();
        var kp = new KafkaProducerManager.ProducerHolder(
            "orders",
            (k, v, c, ct) => producer.ProduceAsync("orders", new Message<Null, string> { Value = (string)v! }, ct),
            _ => producer.Flush(_),
            () => producer.Dispose());

        await kp.SendAsync((object?)null, "value", null, CancellationToken.None);

        Assert.Single(producer.Produced);
        Assert.Equal("orders", producer.Produced[0].Topic);
        Assert.Equal("value", producer.Produced[0].Message.Value);
    }

    [Fact]
    public async Task SendAsync_WithKey_SendsMessage()
    {
        var producer = new DummyKeyedProducer();
        var kp = new KafkaProducerManager.ProducerHolder(
            "orders",
            (k, v, c, ct) => producer.ProduceAsync("orders", new Message<string, string> { Key = (string)k!, Value = (string)v! }, ct),
            _ => producer.Flush(_),
            () => producer.Dispose());

        await kp.SendAsync("1", "value", null, CancellationToken.None);

        Assert.Single(producer.Produced);
        Assert.Equal("1", producer.Produced[0].Message.Key);
        Assert.Equal("value", producer.Produced[0].Message.Value);
    }
}
