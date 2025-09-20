using Kafka.Ksql.Linq.Configuration;
using Confluent.Kafka;
using Kafka.Ksql.Linq.Core.Configuration;
using Xunit;
using System.Threading.Tasks;
using System.Threading;
using System;
using System.Collections.Generic;

#nullable enable

namespace Kafka.Ksql.Linq.Tests.Integration;

[Collection("DataRoundTrip")]
public class ManualCommitIntegrationTests
{
    [Fact]
    [Trait("Category", "Integration")]
    public async Task ManualCommit_PersistsOffset()
    {
        // Ensure clean slate and environment readiness
        try { await PhysicalTestEnv.Cleanup.DeleteSubjectsAsync(EnvManualCommitIntegrationTests.SchemaRegistryUrl, new[] { "manual_commit" }); } catch { }
        try { await PhysicalTestEnv.Cleanup.DeleteTopicsAsync(EnvManualCommitIntegrationTests.KafkaBootstrapServers, new[] { "manual_commit" }); } catch { }
        // Ensure environment is ready and topic exists
        await PhysicalTestEnv.Health.WaitForKafkaAsync(EnvManualCommitIntegrationTests.KafkaBootstrapServers, TimeSpan.FromSeconds(120));
        await PhysicalTestEnv.Health.WaitForHttpOkAsync($"{EnvManualCommitIntegrationTests.SchemaRegistryUrl}/subjects", TimeSpan.FromSeconds(120));
        var groupId = Guid.NewGuid().ToString();

        var options = new KsqlDslOptions
        {
            Common = new CommonSection { BootstrapServers = EnvManualCommitIntegrationTests.KafkaBootstrapServers },
            SchemaRegistry = new SchemaRegistrySection { Url = EnvManualCommitIntegrationTests.SchemaRegistryUrl },
            KsqlDbUrl = EnvManualCommitIntegrationTests.KsqlDbUrl
        };

        options.Topics.Add("manual_commit", new Kafka.Ksql.Linq.Configuration.Messaging.TopicSection
        {
            Consumer = new Kafka.Ksql.Linq.Configuration.Messaging.ConsumerSection
            {
                AutoOffsetReset = "Earliest",
                GroupId = groupId,
                EnableAutoCommit = false
            }
        });

        // produce five messages and commit at the third
        await using (var ctx = new ManualCommitContext(options))
        {
            using var sendCts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            for (var i = 1; i <= 5; i++)
            {
                var attempts = 0;
                while (true)
                {
                    try
                    {
                        await ctx.Samples.AddAsync(new ManualCommitContext.Sample { Id = i }, cancellationToken: sendCts.Token);
                        break;
                    }
                    catch (Confluent.SchemaRegistry.SchemaRegistryException)
                    {
                        if (++attempts >= 3) throw;
                        await Task.Delay(500);
                    }
                }
            }

            using var consumeCts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await ctx.Samples.ForEachAsync((sample, _, _) =>
            {
                if (sample.Id == 3)
                {
                    ctx.Samples.Commit(sample); // manual: 実コミット / autocommit: no-op
                    consumeCts.Cancel();
                }
                return Task.CompletedTask;
            }, autoCommit: false, cancellationToken: consumeCts.Token);
        }
        // verify commit is visible at broker side before re-consuming
        await WaitForCommittedOffsetAsync(
            EnvManualCommitIntegrationTests.KafkaBootstrapServers,
            groupId,
            "manual_commit",
            minOffset: 3,
            timeout: TimeSpan.FromSeconds(30));

        // verify resuming from the committed offset
        await using (var ctx = new ManualCommitContext(options))
        {
            await Task.Delay(2000);
            using var consumeCts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            ManualCommitContext.Sample? received = null;
            await ctx.Samples.ForEachAsync((sample, _, _) =>
            {
                received = sample;
                ctx.Samples.Commit(sample);
                consumeCts.Cancel();
                return Task.CompletedTask;
            }, autoCommit: false, cancellationToken: consumeCts.Token);

            Assert.Equal(4, received!.Id);
        }
    }

    private static async Task WaitForCommittedOffsetAsync(string bootstrap, string groupId, string topic, long minOffset, TimeSpan timeout)
    {
        var cfg = new ConsumerConfig
        {
            BootstrapServers = bootstrap,
            GroupId = groupId,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            AllowAutoCreateTopics = false,
        };
        using var consumer = new ConsumerBuilder<Ignore, Ignore>(cfg).Build();
        var tps = new List<TopicPartition> { new TopicPartition(topic, new Partition(0)) };
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                var committed = consumer.Committed(tps, TimeSpan.FromSeconds(2));
                if (committed != null && committed.Count > 0)
                {
                    var off = committed[0].Offset;
                    if (off != Offset.Unset && off.Value >= minOffset) return;
                }
            }
            catch { }
            await Task.Delay(250);
        }
        // no throw; best-effort wait
    }
}

public static class EnvManualCommitIntegrationTests
{
    internal const string SchemaRegistryUrl = "http://127.0.0.1:18081";
    internal const string KafkaBootstrapServers = "127.0.0.1:39092";
    internal const string KsqlDbUrl = "http://127.0.0.1:18088";
}
