using System;
using System.Threading;
using System.Threading.Tasks;

namespace Kafka.Ksql.Linq.Runtime.Heartbeat;

internal sealed class HeartbeatRunner : IDisposable
{
    private readonly HeartbeatPlanner _planner;
    private readonly KafkaHeartbeatSender _sender;
    private readonly Func<DateTime> _clock;
    private readonly Func<TimeSpan, CancellationToken, Task> _delay;
    private readonly int _jitterMs;
    private CancellationTokenSource? _cts;
    private Task? _task;

    public HeartbeatRunner(
        HeartbeatPlanner planner,
        KafkaHeartbeatSender sender,
        int jitterMs,
        Func<DateTime>? clock = null,
        Func<TimeSpan, CancellationToken, Task>? delay = null)
    {
        _planner = planner;
        _sender = sender;
        _jitterMs = jitterMs;
        _clock = clock ?? (() => DateTime.UtcNow);
        _delay = delay ?? ((t, c) => Task.Delay(t, c));
    }

    public void Start(CancellationToken externalToken)
    {
        if (_task != null) return;
        _cts = CancellationTokenSource.CreateLinkedTokenSource(externalToken);
        var token = _cts.Token;
        _task = Task.Run(async () =>
        {
            while (!token.IsCancellationRequested)
            {
                await DelayToNextMinuteBoundaryWithJitterAsync(token);
                var now = _clock();
                foreach (var hb in _planner.Plan(now))
                    await _sender.TrySendAsync(hb.KeyParts, hb.BucketStartUtc, token);
            }
        }, token);
    }

    private Task DelayToNextMinuteBoundaryWithJitterAsync(CancellationToken ct)
    {
        var now = _clock();
        var ticks = ((now.Ticks / TimeSpan.TicksPerMinute) + 1) * TimeSpan.TicksPerMinute;
        var next = new DateTime(ticks, DateTimeKind.Utc);
        var delay = next - now;
        if (_jitterMs > 0)
            delay += TimeSpan.FromMilliseconds(new Random().Next(_jitterMs));
        return _delay(delay, ct);
    }

    public void Dispose()
    {
        try { _cts?.Cancel(); _task?.Wait(); } catch { }
    }
}
