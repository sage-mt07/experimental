using Kafka.Ksql.Linq.Runtime.Heartbeat;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Runtime.Heartbeat;

public class LeadershipFlagTests
{
    [Fact]
    public void Enable_Disable_Toggles()
    {
        var flag = new LeadershipFlag();
        Assert.False(flag.CanSend);
        flag.Enable();
        Assert.True(flag.CanSend);
        flag.Disable();
        Assert.False(flag.CanSend);
    }
}
