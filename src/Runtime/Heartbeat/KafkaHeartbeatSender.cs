using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Kafka.Ksql.Linq.Runtime.Heartbeat;

internal sealed class KafkaHeartbeatSender
{
    private readonly IProducer<byte[], byte[]> _producer;
    private readonly ILeadershipFlag _flag;
    private readonly string _topic;

    public KafkaHeartbeatSender(IProducer<byte[], byte[]> producer, ILeadershipFlag flag, string topic)
    {
        _producer = producer;
        _flag = flag;
        _topic = topic;
    }

    public async Task TrySendAsync(IReadOnlyList<string> keyParts, DateTime bucketStartUtc, CancellationToken ct)
    {
        if (!_flag.CanSend) return;
        var key = BuildKey(keyParts, bucketStartUtc);
        await _producer.ProduceAsync(_topic, new Message<byte[], byte[]> { Key = key, Value = Array.Empty<byte>() }, ct);
    }

    private static byte[] BuildKey(IReadOnlyList<string> keyParts, DateTime bucketStartUtc)
        => System.Text.Encoding.UTF8.GetBytes(string.Join("|", keyParts) + "|" + bucketStartUtc.ToString("O"));
}

