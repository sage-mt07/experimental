using Confluent.SchemaRegistry;
using Kafka.Ksql.Linq;
using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Configuration.Messaging;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Dlq;
using Kafka.Ksql.Linq.Core.Modeling;
using Kafka.Ksql.Linq.Mapping;
using Kafka.Ksql.Linq.Messaging.Consumers;
using Kafka.Ksql.Linq.Messaging.Producers;
using Kafka.Ksql.Linq.Query.Dsl;
using Kafka.Ksql.Linq.Runtime.Heartbeat;
using Microsoft.Extensions.Options;
using Moq;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Runtime.Heartbeat;

public class KsqlContextRunnerTests
{
    private class Tick
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime Timestamp { get; set; }
    }

    private class TickView
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime BucketStart { get; set; }
    }

    public class MarketSchedule
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime Open { get; set; }
        public DateTime Close { get; set; }
        public DateTime MarketDate { get; set; }
    }

    private class TumblingContext : KsqlContext
    {
        public TumblingContext(KsqlDslOptions? options = null) : base(options ?? new KsqlDslOptions())
        {
            typeof(KsqlContext).GetField("_schemaRegistryClient", BindingFlags.NonPublic | BindingFlags.Instance)!
                .SetValue(this, new Lazy<ISchemaRegistryClient>(() => new Mock<ISchemaRegistryClient>().Object));
        }
        protected override bool SkipSchemaRegistration => true;
        protected override void OnModelCreating(IModelBuilder builder) { }
    }

    private class FakeManager : KafkaConsumerManager
    {
        public bool Called;
        public FakeManager()
            : base(new MappingRegistry(), Options.Create(new KsqlDslOptions()), new(),
                new Mock<IDlqProducer>().Object, new ManualCommitManager(), new LeadershipFlag(), null, new SimpleRateLimiter(0))
        { }
        public override void StartLeaderElectionSafe(string? topic = null, string? groupId = null, string? instanceId = null, CancellationToken ct = default)
        {
            Called = true;
        }
    }

    [Fact(Skip = "Tumbling entities removed from context")]
    public void KsqlContext_StartsLeaderElection_Then_Runner_WhenHasTumbling()
    {
        var ctx = new TumblingContext();
        var fake = new FakeManager();
        typeof(KsqlContext).GetField("_consumerManager", BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(ctx, fake);
        var setMock = new Mock<IEntitySet<MarketSchedule>>();
        setMock.Setup(s => s.ToListAsync(It.IsAny<CancellationToken>())).ReturnsAsync(new List<MarketSchedule>());
        typeof(KsqlContext).GetField("_entitySets", BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(ctx, new Dictionary<Type, object> { { typeof(MarketSchedule), setMock.Object } });
        ctx.StartHeartbeatRunnerAsync(CancellationToken.None).GetAwaiter().GetResult();
        var runner = typeof(KsqlContext).GetField("_hbRunner", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(ctx);
        Assert.NotNull(runner);
        Assert.True(fake.Called);
    }

    [Fact(Skip = "Tumbling entities removed from context")]
    public void ToQuery_WithTumbling_Triggers_MarketSchedule_Load_Once()
    {
        var ctx = new TumblingContext();
        var fake = new FakeManager();
        typeof(KsqlContext).GetField("_consumerManager", BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(ctx, fake);

        var setMock = new Mock<IEntitySet<MarketSchedule>>();
        setMock.Setup(s => s.ToListAsync(It.IsAny<CancellationToken>())).ReturnsAsync(new List<MarketSchedule>());
        typeof(KsqlContext).GetField("_entitySets", BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(ctx, new Dictionary<Type, object> { { typeof(MarketSchedule), setMock.Object } });

        ctx.StartHeartbeatRunnerAsync(CancellationToken.None).GetAwaiter().GetResult();
        ctx.StartHeartbeatRunnerAsync(CancellationToken.None).GetAwaiter().GetResult();
        setMock.Verify(s => s.ToListAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact(Skip = "Tumbling entities removed from context")]
    public void StartsDailyRefresh_And_RefreshesProvider()
    {
        var ctx = new TumblingContext();
        var fake = new FakeManager();
        typeof(KsqlContext).GetField("_consumerManager", BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(ctx, fake);

        var rows = new List<MarketSchedule>();
        var setMock = new Mock<IEntitySet<MarketSchedule>>();
        setMock.Setup(s => s.ToListAsync(It.IsAny<CancellationToken>())).ReturnsAsync(rows);
        typeof(KsqlContext).GetField("_entitySets", BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(ctx, new Dictionary<Type, object> { { typeof(MarketSchedule), setMock.Object } });

        var providerMock = new Mock<IMarketScheduleProvider>();
        providerMock.Setup(p => p.InitializeAsync(typeof(MarketSchedule), It.IsAny<IEnumerable>(), It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
        typeof(KsqlContext).GetField("_marketScheduleProvider", BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(ctx, providerMock.Object);

        var cts = new CancellationTokenSource();
        TimeSpan captured = TimeSpan.Zero;
        var tcs = new TaskCompletionSource();
        providerMock.Setup(p => p.RefreshAsync(typeof(MarketSchedule), rows, It.IsAny<CancellationToken>())).Returns(() => { tcs.SetResult(); cts.Cancel(); return Task.CompletedTask; });
        typeof(KsqlContext).GetField("_now", BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(ctx, (Func<DateTime>)(() => new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc)));
        typeof(KsqlContext).GetField("_delay", BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(ctx, (Func<TimeSpan, CancellationToken, Task>)((t, c) => { captured = t; return Task.CompletedTask; }));

        ctx.StartHeartbeatRunnerAsync(cts.Token).GetAwaiter().GetResult();
        tcs.Task.Wait(1000);
        providerMock.Verify(p => p.RefreshAsync(typeof(MarketSchedule), rows, It.IsAny<CancellationToken>()), Times.Once);
        Assert.Equal(TimeSpan.FromMinutes(5), captured);
    }

    [Fact(Skip = "Tumbling entities removed from context")]
    public void ToListAsync_DoesNotRequire_AppLevel_PK_Hardcode()
    {
        var ctx = new TumblingContext();
        var fake = new FakeManager();
        typeof(KsqlContext).GetField("_consumerManager", BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(ctx, fake);

        var setMock = new Mock<IEntitySet<MarketSchedule>>();
        setMock.Setup(s => s.ToListAsync(It.IsAny<CancellationToken>())).ReturnsAsync(new List<MarketSchedule>());
        typeof(KsqlContext).GetField("_entitySets", BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(ctx, new Dictionary<Type, object> { { typeof(MarketSchedule), setMock.Object } });

        ctx.StartHeartbeatRunnerAsync(CancellationToken.None).GetAwaiter().GetResult();
        setMock.Verify(s => s.ToListAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public void StartHeartbeatRunner_Uses_Grace_From_Options()
    {
        var opts = new KsqlDslOptions { Heartbeat = new HeartbeatOptions { Grace = TimeSpan.FromSeconds(5) } };
        var ctx = new TumblingContext(opts);
        var fake = new FakeManager();
        typeof(KsqlContext).GetField("_consumerManager", BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(ctx, fake);

        var setMock = new Mock<IEntitySet<MarketSchedule>>();
        setMock.Setup(s => s.ToListAsync(It.IsAny<CancellationToken>())).ReturnsAsync(new List<MarketSchedule>());
        typeof(KsqlContext).GetField("_entitySets", BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(ctx, new Dictionary<Type, object> { { typeof(MarketSchedule), setMock.Object } });

        var qm = new KsqlQueryModel { BasedOnType = typeof(MarketSchedule) };
        qm.Windows.Add("1");
        var model = new EntityModel { EntityType = typeof(Tick), QueryModel = qm };
        var models = new ConcurrentDictionary<Type, EntityModel>();
        models[typeof(Tick)] = model;
        typeof(KsqlContext).GetField("_entityModels", BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(ctx, models);

        var provider = new Mock<IMarketScheduleProvider>();
        provider.Setup(p => p.InitializeAsync(typeof(MarketSchedule), It.IsAny<IEnumerable>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        typeof(KsqlContext).GetField("_marketScheduleProvider", BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(ctx, provider.Object);

        ctx.StartHeartbeatRunnerAsync(CancellationToken.None).GetAwaiter().GetResult();

        var runner = (HeartbeatRunner)typeof(KsqlContext).GetField("_hbRunner", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(ctx)!;
        var planner = (HeartbeatPlanner)typeof(HeartbeatRunner).GetField("_planner", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(runner)!;
        var grace = (TimeSpan)typeof(HeartbeatPlanner).GetField("_grace", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(planner)!;
        Assert.Equal(TimeSpan.FromSeconds(5), grace);
    }
}
