using Confluent.Kafka;
using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Configuration.Abstractions;
using Kafka.Ksql.Linq.Configuration.Messaging;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Dlq;
using Kafka.Ksql.Linq.Mapping;
using Kafka.Ksql.Linq.Messaging;
using Kafka.Ksql.Linq.Messaging.Consumers;
using Kafka.Ksql.Linq.Messaging.Producers;
using Kafka.Ksql.Linq.Runtime.Heartbeat;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using static Kafka.Ksql.Linq.Tests.PrivateAccessor;

#nullable enable

namespace Kafka.Ksql.Linq.Tests.Messaging;

public class KafkaConsumerManagerTests
{
    private class SampleEntity
    {
        public int Id { get; set; }
    }

    [Fact]
    public void BuildConsumerConfig_PrefersAppsettingsThenFluentThenAttribute()
    {
        var options = new KsqlDslOptions
        {
            Common = new CommonSection { BootstrapServers = "s", ClientId = "c" },
            Topics = new Dictionary<string, TopicSection>
            {
                ["t"] = new TopicSection
                {
                    Consumer = new ConsumerSection { GroupId = "cfg", EnableAutoCommit = true }
                }
            }
        };
        var manager = new KafkaConsumerManager(
            new MappingRegistry(),
            Options.Create(options),
            new ConcurrentDictionary<Type, EntityModel>(),
            new Mock<IDlqProducer>().Object,
            new ManualCommitManager(),
            new LeadershipFlag(),
            new NullLoggerFactory(),
            new SimpleRateLimiter(0));

        var sub = new KafkaSubscriptionOptions { GroupId = "fluent" };
        var config = InvokePrivate<ConsumerConfig>(manager, "BuildConsumerConfig",
            new[] { typeof(string), typeof(KafkaSubscriptionOptions), typeof(string), typeof(bool) },
            null, "t", sub, "attr", false);

        Assert.Equal("cfg", config.GroupId);
        Assert.True(config.EnableAutoCommit);

        options.Topics.Clear();
        config = InvokePrivate<ConsumerConfig>(manager, "BuildConsumerConfig",
            new[] { typeof(string), typeof(KafkaSubscriptionOptions), typeof(string), typeof(bool) },
            null, "t", sub, "attr", false);
        Assert.Equal("fluent", config.GroupId);
        Assert.False(config.EnableAutoCommit);

        config = InvokePrivate<ConsumerConfig>(manager, "BuildConsumerConfig",
            new[] { typeof(string), typeof(KafkaSubscriptionOptions), typeof(string), typeof(bool) },
            null, "t", null, "attr", true);
        Assert.Equal("attr", config.GroupId);
    }
    //[Fact]
    //public void BuildConsumerConfig_ReturnsConfiguredValues()
    //{
    //    var options = new KsqlDslOptions
    //    {
    //        Common = new CommonSection { BootstrapServers = "server", ClientId = "cid" },
    //        Topics = new Dictionary<string, TopicSection>
    //        {
    //            ["topic"] = new TopicSection
    //            {
    //                Consumer = new ConsumerSection
    //                {
    //                    GroupId = "gid",
    //                    AutoOffsetReset = "Earliest",
    //                    EnableAutoCommit = false,
    //                    AutoCommitIntervalMs = 100,
    //                    SessionTimeoutMs = 200,
    //                    HeartbeatIntervalMs = 300,
    //                    MaxPollIntervalMs = 400,
    //                    FetchMinBytes = 5,
    //                    FetchMaxBytes = 10,
    //                    IsolationLevel = "ReadCommitted",
    //                    AdditionalProperties = new Dictionary<string,string>{{"p","v"}}
    //                }
    //            }
    //        }
    //    };
    //    var manager = new KafkaConsumerManager(
    //        new MappingRegistry(),
    //        Options.Create(options),
    //        new Dictionary<Type, EntityModel>(),
    //        new Mock<IDlqProducer>().Object,
    //        new NullLoggerFactory(),
    //        new SimpleRateLimiter(0));
    //    var config = InvokePrivate<ConsumerConfig>(manager, "BuildConsumerConfig", new[] { typeof(string), typeof(KafkaSubscriptionOptions) }, null, "topic", null);

    //    Assert.Equal("server", config.BootstrapServers);
    //    Assert.Equal("cid", config.ClientId);
    //    Assert.Equal("gid", config.GroupId);
    //    Assert.Equal(AutoOffsetReset.Earliest, config.AutoOffsetReset);
    //    Assert.False(config.EnableAutoCommit);
    //    Assert.Equal(100, config.AutoCommitIntervalMs);
    //    Assert.Equal(200, config.SessionTimeoutMs);
    //    Assert.Equal(300, config.HeartbeatIntervalMs);
    //    Assert.Equal(400, config.MaxPollIntervalMs);
    //    Assert.Equal(5, config.FetchMinBytes);
    //    Assert.Equal(10, config.FetchMaxBytes);
    //    Assert.Equal(IsolationLevel.ReadCommitted, config.IsolationLevel);
    //    Assert.Equal("v", config.Get("p"));
    //}


    [Fact]
    public void GetEntityModel_ThrowsWhenModelMissing()
    {
        var options = Options.Create(new KsqlDslOptions());
        var manager = new KafkaConsumerManager(
            new MappingRegistry(),
            options,
            new ConcurrentDictionary<Type, EntityModel>(),
            new Mock<IDlqProducer>().Object,
            new ManualCommitManager(),
            new LeadershipFlag(),
            new NullLoggerFactory(),
            new SimpleRateLimiter(0));

        Assert.Throws<InvalidOperationException>(() =>
            InvokePrivate<Kafka.Ksql.Linq.Core.Abstractions.EntityModel>(manager, "GetEntityModel", Type.EmptyTypes, new[] { typeof(SampleEntity) }));
    }

    [Fact]
    public void TryGetSchemaId_ParsesMagicByte()
    {
        var bytes = new byte[5];
        bytes[0] = 0;
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(1, 4), 42);
        var id = InvokePrivate<int?>(typeof(KafkaConsumerManager), "TryGetSchemaId", new[] { typeof(byte[]) }, null, bytes);
        Assert.Equal(42, id);
    }

    [Fact]
    public void ExtractAllowedHeaders_FiltersAndEncodes()
    {
        var headers = new Headers
        {
            new Header("x-correlation-id", System.Text.Encoding.UTF8.GetBytes("abc")),
            new Header("traceparent", new byte[]{0xff,0xfe}),
            new Header("ignore", System.Text.Encoding.UTF8.GetBytes("x"))
        };
        var allow = new[] { "x-correlation-id", "traceparent" };
        var dict = InvokePrivate<System.Collections.Generic.IReadOnlyDictionary<string, string>>(typeof(KafkaConsumerManager),
            "ExtractAllowedHeaders",
            new[] { typeof(Headers), typeof(System.Collections.Generic.IEnumerable<string>), typeof(int) },
            null,
            headers, allow, 10);
        Assert.Equal("abc", dict["x-correlation-id"]);
        Assert.StartsWith("base64:", dict["traceparent"]);
        Assert.False(dict.ContainsKey("ignore"));
    }

    [Fact]
    public void AssignPartition0_Sets_IsLeaderTrue()
    {
        var flag = new LeadershipFlag();
        var manager = new KafkaConsumerManager(new MappingRegistry(), Options.Create(new KsqlDslOptions()), new(), new Mock<IDlqProducer>().Object, new ManualCommitManager(), flag, new NullLoggerFactory(), new SimpleRateLimiter(0));
        typeof(KafkaConsumerManager).GetField("_hbTopicName", BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(manager, "hb_1m");
        var parts = new List<TopicPartition> { new TopicPartition("hb_1m", new Partition(0)) };
        InvokePrivate(manager, "HandlePartitionsAssigned", new[] { typeof(IReadOnlyList<TopicPartition>) }, null, parts);
        Assert.True(manager.IsLeader);
    }

    [Fact]
    public void Revoke_Sets_IsLeaderFalse()
    {
        var flag = new LeadershipFlag();
        var manager = new KafkaConsumerManager(new MappingRegistry(), Options.Create(new KsqlDslOptions()), new(), new Mock<IDlqProducer>().Object, new ManualCommitManager(), flag, new NullLoggerFactory(), new SimpleRateLimiter(0));
        typeof(KafkaConsumerManager).GetField("_hbTopicName", BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(manager, "hb_1m");
        var parts = new List<TopicPartition> { new TopicPartition("hb_1m", new Partition(0)) };
        InvokePrivate(manager, "HandlePartitionsAssigned", new[] { typeof(IReadOnlyList<TopicPartition>) }, null, parts);
        var revoked = new List<TopicPartitionOffset> { new TopicPartitionOffset("hb_1m", new Partition(0), Offset.Beginning) };
        InvokePrivate(manager, "HandlePartitionsRevoked", new[] { typeof(IReadOnlyList<TopicPartitionOffset>) }, null, revoked);
        Assert.False(manager.IsLeader);
    }

    [Fact]
    public void AssignNonZero_DoesNotAffect_IsLeader()
    {
        var flag = new LeadershipFlag();
        var manager = new KafkaConsumerManager(new MappingRegistry(), Options.Create(new KsqlDslOptions()), new(), new Mock<IDlqProducer>().Object, new ManualCommitManager(), flag, new NullLoggerFactory(), new SimpleRateLimiter(0));
        typeof(KafkaConsumerManager).GetField("_hbTopicName", BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(manager, "hb_1m");
        var parts = new List<TopicPartition> { new TopicPartition("hb_1m", new Partition(1)) };
        InvokePrivate(manager, "HandlePartitionsAssigned", new[] { typeof(IReadOnlyList<TopicPartition>) }, null, parts);
        Assert.False(manager.IsLeader);
    }

    [Fact]
    public void DataTopic_Assign_DoesNotAffect_IsLeader()
    {
        var flag = new LeadershipFlag();
        var manager = new KafkaConsumerManager(new MappingRegistry(), Options.Create(new KsqlDslOptions()), new(), new Mock<IDlqProducer>().Object, new ManualCommitManager(), flag, new NullLoggerFactory(), new SimpleRateLimiter(0));
        typeof(KafkaConsumerManager).GetField("_hbTopicName", BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(manager, "hb_1m");
        var parts = new List<TopicPartition> { new TopicPartition("rates", new Partition(0)) };
        InvokePrivate(manager, "HandlePartitionsAssigned", new[] { typeof(IReadOnlyList<TopicPartition>) }, null, parts);
        Assert.False(manager.IsLeader);
    }

    [Fact]
    public void ConsumeAsync_DefaultFromBeginningIsFalse()
    {
        var options = Options.Create(new KsqlDslOptions());
        var manager = new KafkaConsumerManager(new MappingRegistry(), options, new(), new Mock<IDlqProducer>().Object, new ManualCommitManager(), new LeadershipFlag(), new NullLoggerFactory(), new SimpleRateLimiter(0));
        var _ = manager.ConsumeAsync<SampleEntity>();
    }

    [Fact]
    public async Task HandleMappingException_ProducesAndCommits()
    {
        var dlq = new Mock<IDlqProducer>();
        var consumer = new Mock<IConsumer<byte[], byte[]>>();
        var result = new ConsumeResult<byte[], byte[]>
        {
            Topic = "t",
            Partition = new Partition(0),
            Offset = new Offset(1),
            Message = new Message<byte[], byte[]>
            {
                Key = new byte[0],
                Value = new byte[0],
                Headers = new Headers(),
                Timestamp = new Timestamp(DateTime.UtcNow)
            }
        };
        var options = new DlqOptions();
        var limiter = new SimpleRateLimiter(0);

        await KafkaConsumerManager.HandleMappingException(result, new Exception("e"), dlq.Object, consumer.Object, options, limiter, CancellationToken.None);

        dlq.Verify(p => p.ProduceAsync(It.Is<DlqEnvelope>(e => e.Topic == "t" && e.Offset == 1), It.IsAny<CancellationToken>()), Times.Once);
        consumer.Verify(c => c.Commit(result), Times.Once);
    }

}
