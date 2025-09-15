using Confluent.Kafka;
using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Configuration.Messaging;
using Kafka.Ksql.Linq.Mapping;
using Kafka.Ksql.Linq.Messaging.Producers;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using Xunit;
using static Kafka.Ksql.Linq.Tests.PrivateAccessor;

#nullable enable

namespace Kafka.Ksql.Linq.Tests.Messaging;

public class KafkaProducerManagerTests
{
    private class SampleEntity
    {
        public int Id { get; set; }
    }


    [Fact(Skip = "Requires full producer configuration")]
    public void BuildProducerConfig_ReturnsConfiguredValues()
    {
        var options = new KsqlDslOptions
        {
            Common = new CommonSection { BootstrapServers = "server", ClientId = "cid" },
            Topics = new Dictionary<string, TopicSection>
            {
                ["topic"] = new TopicSection
                {
                    Producer = new ProducerSection
                    {
                        Acks = "All",
                        CompressionType = "Gzip",
                        EnableIdempotence = false,
                        MaxInFlightRequestsPerConnection = 2,
                        LingerMs = 10,
                        BatchSize = 1000,
                        RetryBackoffMs = 200
                    }
                }
            }
        };
        var manager = new KafkaProducerManager(new MappingRegistry(), Options.Create(options), new NullLoggerFactory());
        var config = InvokePrivate<ProducerConfig>(manager, "BuildProducerConfig", new[] { typeof(string) }, null, "topic");

        Assert.Equal("server", config.BootstrapServers);
        Assert.Equal("cid", config.ClientId);
        Assert.Equal(Acks.All, config.Acks);
        Assert.Equal(CompressionType.Gzip, config.CompressionType);
        Assert.False(config.EnableIdempotence);
        Assert.Equal(2, config.MaxInFlight);
        Assert.Equal(10, config.LingerMs);
        Assert.Equal(1000, config.BatchSize);
        Assert.Equal(200, config.RetryBackoffMs);
    }


    [Fact]
    public void GetEntityModel_ReturnsModelWithAttributes()
    {
        var options = Options.Create(new KsqlDslOptions());
        var manager = new KafkaProducerManager(new MappingRegistry(), options, new NullLoggerFactory());
        var model = InvokePrivate<Kafka.Ksql.Linq.Core.Abstractions.EntityModel>(manager, "GetEntityModel", Type.EmptyTypes, new[] { typeof(SampleEntity) });
        Assert.Equal(typeof(SampleEntity), model.EntityType);
        Assert.Empty(model.KeyProperties);
        Assert.Equal("sampleentity", model.TopicName);
    }
}
