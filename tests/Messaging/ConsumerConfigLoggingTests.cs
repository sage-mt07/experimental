using System;
using System.Collections.Concurrent;
using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Mapping;
using Kafka.Ksql.Linq.Messaging.Consumers;
using Kafka.Ksql.Linq.Messaging.Producers;
using Kafka.Ksql.Linq.Runtime.Heartbeat;
using Kafka.Ksql.Linq.Tests.Utils;
using Microsoft.Extensions.Options;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Messaging;

public class ConsumerConfigLoggingTests
{
    private static (KafkaConsumerManager mgr, TestLoggerFactory lf) Build(bool enableAutoCommit)
    {
        var opts = new KsqlDslOptions
        {
            Common = new CommonSection { BootstrapServers = "localhost:9092", ClientId = "ut" },
            SchemaRegistry = new Kafka.Ksql.Linq.Core.Configuration.SchemaRegistrySection { Url = "http://localhost:8081" }
        };
        opts.Topics["test-topic"] = new Kafka.Ksql.Linq.Configuration.Messaging.TopicSection
        {
            Consumer = new Kafka.Ksql.Linq.Configuration.Messaging.ConsumerSection
            {
                GroupId = "ut-group",
                EnableAutoCommit = enableAutoCommit,
                AutoOffsetReset = "Earliest"
            }
        };
        var mapping = new MappingRegistry();
        var models = new ConcurrentDictionary<Type, EntityModel>();
        var model = new EntityModel { EntityType = typeof(object), TopicName = "test-topic" };
        models[typeof(object)] = model;
        var dlqProd = new DlqProducer(new Kafka.Ksql.Linq.Messaging.Producers.KafkaProducerManager(mapping, Options.Create(opts)), "dlq");
        var flag = new LeadershipFlag();
        var lf = new TestLoggerFactory();
        var mgr = new KafkaConsumerManager(mapping, Options.Create(opts), models, dlqProd, new ManualCommitManager(), flag, lf);
        return (mgr, lf);
    }

    [Fact]
    public void BuildConsumerConfig_LogsInformation()
    {
        var (mgr, lf) = Build(enableAutoCommit: true);
        // invoke private BuildConsumerConfig via reflection
        var cfg = PrivateAccessor.InvokePrivate<Confluent.Kafka.ConsumerConfig>(
            mgr,
            name: "BuildConsumerConfig",
            parameterTypes: new[] { typeof(string), typeof(Kafka.Ksql.Linq.Configuration.Abstractions.KafkaSubscriptionOptions), typeof(string), typeof(bool) },
            args: new object?[] { "test-topic", new Kafka.Ksql.Linq.Configuration.Abstractions.KafkaSubscriptionOptions(), null, true }
        );
        Assert.NotNull(cfg);
        // assert that Information log for consumer:test-topic config exists
        var found = false;
        foreach (var kv in lf.Loggers)
        {
            foreach (var e in kv.Value.Entries)
            {
                if (e.Level == Microsoft.Extensions.Logging.LogLevel.Information && e.Message.Contains("consumer:test-topic config:"))
                {
                    found = true;
                    break;
                }
            }
            if (found) break;
        }
        Assert.True(found, "Expected Information log for consumer:test-topic config");
    }
}

