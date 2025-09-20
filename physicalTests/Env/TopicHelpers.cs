using Confluent.Kafka;
using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace PhysicalTestEnv;

public static class TopicHelpers
{
    public static async Task WaitForTopicReady(IAdminClient admin, string topic, int partitions, short rf, TimeSpan timeout, CancellationToken ct = default)
    {
        var sw = Stopwatch.StartNew();
        var backoffMs = 100;
        while (sw.Elapsed < timeout && !ct.IsCancellationRequested)
        {
            try
            {
                var md = admin.GetMetadata(topic, TimeSpan.FromSeconds(2));
                var t = md?.Topics?.FirstOrDefault();
                if (t is not null && t.Error.Code == ErrorCode.NoError && t.Partitions.Count == partitions)
                {
                    var allReady = t.Partitions.All(p => p.Leader >= 0 && p.Replicas.Length >= rf && p.InSyncReplicas.Length >= rf);
                    if (allReady) return;
                }
            }
            catch (KafkaException)
            {
                // transient during startup; retry
            }
            await Task.Delay(backoffMs, ct);
            backoffMs = Math.Min((int)(backoffMs * 1.5) + Random.Shared.Next(30), 2000);
        }
        throw new TimeoutException($"Topic {topic} readiness timeout after {timeout}.");
    }
}

