using Confluent.Kafka;
using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Configuration.Messaging;
using Kafka.Ksql.Linq.Infrastructure.Admin;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using PhysicalTestEnv;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Integration;

public class KafkaAdminServiceIntegrationTests
{
    [Fact]
    [Trait("Category", "Integration")]
    public async Task EnsureTopic_CreatesWithConfiguredStructure()
    {
        await Health.WaitForKafkaAsync(EnvKafkaAdminServiceIntegrationTests.KafkaBootstrapServers, TimeSpan.FromSeconds(60));
        var options = new KsqlDslOptions
        {
            Common = new CommonSection { BootstrapServers = EnvKafkaAdminServiceIntegrationTests.KafkaBootstrapServers },
            Topics =
            {
                ["it.topic"] = new TopicSection
                {
                    Creation = new TopicCreationSection { NumPartitions = 1, ReplicationFactor = 1 }
                }
            }
        };
        var svc = new KafkaAdminService(Options.Create(options), NullLoggerFactory.Instance);
        await svc.EnsureTopicExistsAsync("it.topic");
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = EnvKafkaAdminServiceIntegrationTests.KafkaBootstrapServers }).Build();
        var meta = admin.GetMetadata("it.topic", TimeSpan.FromSeconds(10));
        Assert.Single(meta.Topics[0].Partitions);
    }
}

public static class EnvKafkaAdminServiceIntegrationTests
{
    internal const string KafkaBootstrapServers = "127.0.0.1:39092";
}
