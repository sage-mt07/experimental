using System.Threading;

namespace Kafka.Ksql.Linq.Runtime.Heartbeat;

public interface ILeadershipFlag
{
    bool CanSend { get; }
    void Enable();
    void Disable();
}

internal sealed class LeadershipFlag : ILeadershipFlag
{
    private int _canSend;

    public bool CanSend => Volatile.Read(ref _canSend) != 0;

    public void Enable() => Interlocked.Exchange(ref _canSend, 1);

    public void Disable() => Interlocked.Exchange(ref _canSend, 0);
}
