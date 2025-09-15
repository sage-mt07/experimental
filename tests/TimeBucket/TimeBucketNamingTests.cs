using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Ksql.Linq.Runtime;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Unit;

public class TimeBucketNamingTests
{
    private sealed class DummyCtx : ITimeBucketContext
    {
        private sealed class DummySet<T> : ITimeBucketSet<T> where T : class
        {
            public Task<List<T>> ToListAsync(IReadOnlyList<string> pkFilter, CancellationToken ct)
                => Task.FromResult(new List<T>());
        }
        public ITimeBucketSet<T> Set<T>(string topic, Period period) where T : class
            => new DummySet<T>();
    }

    private class Bar { }

    [Fact]
    public void LiveTopicName_1m_And_5m_Are_Correct()
    {
        var ctx = new DummyCtx();
        var b1 = TimeBucket.Get<Bar>(ctx, Period.Minutes(1));
        var b5 = TimeBucket.Get<Bar>(ctx, Period.Minutes(5));

        Assert.Equal("bar_1m_live", b1.LiveTopicName);
        Assert.Equal("bar_5m_live", b5.LiveTopicName);
        Assert.Null(b1.FinalTopicName);
        Assert.Null(b5.FinalTopicName);
    }

    [Fact]
    public void FinalTopicName_1s_Is_Correct()
    {
        var ctx = new DummyCtx();
        var b = TimeBucket.Get<Bar>(ctx, Period.Seconds(1));
        Assert.Equal("bar_1s_final", b.FinalTopicName);
        Assert.Null(b.LiveTopicName);
    }
}

