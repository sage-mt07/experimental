using Kafka.Ksql.Linq.Core.Models;
using Kafka.Ksql.Linq.Mapping;
using Kafka.Ksql.Linq.Runtime.Heartbeat;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Runtime.Heartbeat;

public class MarketScheduleProviderTests
{
    private static MarketScheduleProvider CreateProvider()
    {
        var registry = new MappingRegistry();
        var keyProps = new[]
        {
            PropertyMeta.FromProperty(typeof(Schedule).GetProperty(nameof(Schedule.Broker))!),
            PropertyMeta.FromProperty(typeof(Schedule).GetProperty(nameof(Schedule.Symbol))!)
        };
        var valueProps = typeof(Schedule).GetProperties().Select(p => PropertyMeta.FromProperty(p)).ToArray();
        registry.Register(typeof(Schedule), keyProps, valueProps);
        return new MarketScheduleProvider(registry);
    }

    private record Schedule(string Broker, string Symbol, DateTime Open, DateTime Close);

    [Fact]
    public async Task MarketScheduleProvider_Loads_And_Indexes_With_ToListAsync()
    {
        var provider = CreateProvider();
        var rows = new[] { new Schedule("b", "s", new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(2025, 1, 1, 1, 0, 0, DateTimeKind.Utc)) };
        await provider.InitializeAsync(typeof(Schedule), rows, CancellationToken.None);
        Assert.True(provider.IsInSession(new[] { "b", "s" }, new DateTime(2025, 1, 1, 0, 30, 0, DateTimeKind.Utc)));
    }

    [Fact]
    public async Task IsInSession_OpenInclusive_CloseExclusive_Boundaries()
    {
        var provider = CreateProvider();
        var open = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var close = new DateTime(2025, 1, 1, 1, 0, 0, DateTimeKind.Utc);
        await provider.InitializeAsync(typeof(Schedule), new[] { new Schedule("b", "s", open, close) }, CancellationToken.None);
        Assert.True(provider.IsInSession(new[] { "b", "s" }, open));
        Assert.False(provider.IsInSession(new[] { "b", "s" }, close));
    }
}

