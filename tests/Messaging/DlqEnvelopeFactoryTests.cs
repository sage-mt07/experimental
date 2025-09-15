using Confluent.Kafka;
using Kafka.Ksql.Linq.Messaging;
using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Messaging;

public class DlqEnvelopeFactoryTests
{
    [Fact]
    public void FromConsumeResult_ExtractsAvroSchemaAndHeaders()
    {
        var value = new byte[5];
        value[0] = 0; BinaryPrimitives.WriteInt32BigEndian(value.AsSpan(1), 10);
        var cr = new ConsumeResult<byte[], byte[]>
        {
            Topic = "t",
            Partition = 1,
            Offset = 2,
            Message = new Message<byte[], byte[]>
            {
                Key = value,
                Value = value,
                Timestamp = new Timestamp(new DateTime(2020, 1, 1), TimestampType.CreateTime)
            }
        };
        var headers = new Dictionary<string, string>
        {
            ["x-correlation-id"] = "abc",
            ["binary"] = "base64:AAEC"
        };
        var env = DlqEnvelopeFactory.From(cr, new Exception("err"), "app", "cg", "host", headers);
        Assert.Equal("10", env.SchemaIdValue);
        Assert.Equal("avro", env.PayloadFormatValue);
        Assert.Equal("abc", env.Headers["x-correlation-id"]);
        Assert.Equal("base64:AAEC", env.Headers["binary"]);
    }

    [Fact]
    public void FromConsumeResult_NonAvroFormatsAreNone()
    {
        var cr = new ConsumeResult<string, string>
        {
            Topic = "t",
            Partition = 0,
            Offset = 0,
            Message = new Message<string, string>
            {
                Key = "k",
                Value = "v",
                Timestamp = new Timestamp(DateTime.UtcNow, TimestampType.CreateTime)
            }
        };
        var env = DlqEnvelopeFactory.From(cr, new Exception("e"), null, null, null, new Dictionary<string, string>());
        Assert.Empty(env.SchemaIdKey);
        Assert.Equal("none", env.PayloadFormatKey);
        Assert.Equal("none", env.PayloadFormatValue);
    }

    [Fact]
    public void FromMessageMeta_TrimsLongMessage()
    {
        var meta = new MessageMeta("t", 0, 0, DateTimeOffset.UnixEpoch, null, null, false, new Dictionary<string, string>());
        var longMsg = new string('a', 50);
        var env = DlqEnvelopeFactory.From(meta, new Exception(longMsg), null, null, null, maxMsg: 10);
        Assert.Equal(10, env.ErrorMessageShort.Length);
    }

    [Fact]
    public void FromMessageMeta_StacklessStillHasFingerprint()
    {
        var meta = new MessageMeta("t", 0, 0, DateTimeOffset.UnixEpoch, null, null, false, new Dictionary<string, string>());
        var ex = new Exception("e");
        var env = DlqEnvelopeFactory.From(meta, ex, null, null, null);
        Assert.Null(env.StackTraceShort);
        Assert.NotEmpty(env.ErrorFingerprint);
    }
}
