using Confluent.Kafka;
using Kafka.Ksql.Linq.Runtime.Heartbeat;
using Moq;
using System;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Runtime.Heartbeat;

public class HeartbeatRunnerTests
{
    private sealed class FakeClock
    {
        public DateTime Now;
    }

    [Fact]
    public async Task Runner_Aligns_To_MinuteBoundary_And_AfterGrace()
    {
        var clock = new FakeClock { Now = new DateTime(2025, 1, 1, 0, 1, 0, DateTimeKind.Utc) };
        Func<DateTime> now = () => clock.Now;
        TimeSpan? observed = null;
        Func<TimeSpan, CancellationToken, Task> delay = (t, _) => { observed = t; clock.Now += t; return Task.CompletedTask; };
        var start = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var item = new HeartbeatItem(new[] { "b", "s" }, start);
        var provider = new Mock<IMarketScheduleProvider>();
        provider.Setup(p => p.IsInSession(new[] { "b", "s" }, start)).Returns(true);
        var planner = new HeartbeatPlanner(TimeSpan.Zero, new[] { item }, provider.Object);
        var flag = new LeadershipFlag();
        flag.Enable();
        var producer = new Mock<IProducer<byte[], byte[]>>();
        var sender = new KafkaHeartbeatSender(producer.Object, flag, "hb_1m");
        var runner = new HeartbeatRunner(planner, sender, 0, now, delay);
        runner.Start(CancellationToken.None);
        await Task.Delay(10);
        runner.Dispose();
        producer.Verify(p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<byte[], byte[]>>(), It.IsAny<CancellationToken>()), Times.AtLeastOnce());
        Assert.Equal(TimeSpan.FromMinutes(1), observed);
    }
}
