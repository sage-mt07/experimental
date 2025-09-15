using Kafka.Ksql.Linq.Runtime.Heartbeat;
using System;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Runtime.Heartbeat;

public class HeartbeatPlannerTests
{
    [Fact]
    public void Planner_Respects_Grace_And_Session()
    {
        var k = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var items = new[]
        {
            new HeartbeatItem(new[]{"b","s"}, k),
            new HeartbeatItem(new[]{"b","s"}, k.AddMinutes(1)),
            new HeartbeatItem(new[]{"b","x"}, k)
        };
        var provider = new Moq.Mock<IMarketScheduleProvider>();
        provider.Setup(p => p.IsInSession(new[] { "b", "s" }, k)).Returns(true);
        provider.Setup(p => p.IsInSession(new[] { "b", "s" }, k.AddMinutes(1))).Returns(true);
        provider.Setup(p => p.IsInSession(new[] { "b", "x" }, k)).Returns(false);
        var planner = new HeartbeatPlanner(TimeSpan.FromMinutes(1), items, provider.Object);
        var now = k.AddMinutes(2);
        var result = planner.Plan(now);
        Assert.Collection(result, hb => Assert.Equal("s", hb.KeyParts[1]));
    }
}
