using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Query.Analysis;
using Kafka.Ksql.Linq.Query.Dsl;
using System;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Analysis;

public class DerivationPlannerTests
{
    [KsqlTopic("bar")]
    private class Source
    {
        public int Id { get; set; }
    }

    [KsqlTopic("bar")]
    private class SourceOhlc
    {
        public int Id { get; set; }
        public double Open { get; set; }
        public double High { get; set; }
        public double Low { get; set; }
        public double KsqlTimeFrameClose { get; set; }
    }

    private static TumblingQao Create(params Timeframe[] tfs) => new()
    {
        TimeKey = "Timestamp",
        Windows = tfs,
        Keys = new[] { "Id", "BucketStart" },
        Projection = new[] { "Id", "BucketStart", "KsqlTimeFrameClose" },
        PocoShape = new[]
        {
            new ColumnShape("Id", typeof(int), false),
            new ColumnShape("BucketStart", typeof(long), false),
            new ColumnShape("KsqlTimeFrameClose", typeof(double), false)
        },
        BasedOn = new BasedOnSpec(new[] { "Id", "BucketStart" }, string.Empty, "KsqlTimeFrameClose", string.Empty),
        WeekAnchor = DayOfWeek.Monday
    };

    [Fact]
    public void Plan_1m_Includes_Live_Hb()
    {
        var model = new EntityModel { EntityType = typeof(Source) };
        var entities = DerivationPlanner.Plan(Create(new Timeframe(1, "m")), model, whenEmpty: true);
        var final1s = Assert.Single(entities, e => e.Id == "bar_1s_final" && e.Role == Role.Final1s);
        var final1sStream = Assert.Single(entities, e => e.Id == "bar_1s_final_s" && e.Role == Role.Final1sStream);
        var live = Assert.Single(entities, e => e.Id == "bar_1m_live" && e.Role == Role.Live);
        Assert.Equal("bar_1s_final_s", live.InputHint);
        Assert.DoesNotContain(entities, e => e.Id == "bar_1m_final");
        Assert.Contains(entities, e => e.Id == "bar_hb_1m" && e.Role == Role.Hb);
        Assert.Contains(entities, e => e.Id == "bar_hb_1s" && e.Role == Role.Hb);
    }

    [Fact]
    public void Plan_5m_Includes_1s_Hub()
    {
        var model = new EntityModel { EntityType = typeof(Source) };
        var entities = DerivationPlanner.Plan(Create(new Timeframe(5, "m")), model, whenEmpty: true);
        var final1s = Assert.Single(entities, e => e.Id == "bar_1s_final" && e.Role == Role.Final1s);
        var final1sStream = Assert.Single(entities, e => e.Id == "bar_1s_final_s" && e.Role == Role.Final1sStream);
        var live5 = Assert.Single(entities, e => e.Id == "bar_5m_live" && e.Role == Role.Live);
        Assert.Equal("bar_1s_final_s", live5.InputHint);
        Assert.DoesNotContain(entities, e => e.Id == "bar_5m_final");
        Assert.Contains(entities, e => e.Id == "bar_hb_5m" && e.Role == Role.Hb);
    }

    [Fact]
    public void Plan_Windows_Live_Use_Hub_Input()
    {
        var model = new EntityModel { EntityType = typeof(Source) };
        var entities = DerivationPlanner.Plan(Create(new Timeframe(1, "m"), new Timeframe(5, "m")), model);
        var live1 = Assert.Single(entities, e => e.Id == "bar_1m_live" && e.Role == Role.Live);
        Assert.Equal("bar_1s_final_s", live1.InputHint);
        var live5 = Assert.Single(entities, e => e.Id == "bar_5m_live" && e.Role == Role.Live);
        Assert.Equal("bar_1s_final_s", live5.InputHint);
        Assert.DoesNotContain(entities, e => e.Id == "bar_1m_final" || e.Id == "bar_5m_final");
    }

    [Fact]
    public void Plan_1wk_Live_Uses_1s_Hub()
    {
        var model = new EntityModel { EntityType = typeof(Source) };
        var entities = DerivationPlanner.Plan(Create(new Timeframe(1, "wk")), model);
        var live = Assert.Single(entities, e => e.Id == "bar_1wk_live" && e.Role == Role.Live);
        Assert.Equal("bar_1s_final_s", live.InputHint);
    }

    [Fact]
    public void Plan_Hour_Windows_Use_Hub()
    {
        var model = new EntityModel { EntityType = typeof(Source) };
        var entities = DerivationPlanner.Plan(Create(new Timeframe(3, "h"), new Timeframe(1, "h")), model);
        var live1 = Assert.Single(entities, e => e.Id == "bar_1h_live" && e.Role == Role.Live);
        Assert.Equal("bar_1s_final_s", live1.InputHint);
        var live3 = Assert.Single(entities, e => e.Id == "bar_3h_live" && e.Role == Role.Live);
        Assert.Equal("bar_1s_final_s", live3.InputHint);
    }

    [Fact]
    public void Plan_Day_Windows_Use_Hub()
    {
        var model = new EntityModel { EntityType = typeof(Source) };
        var entities = DerivationPlanner.Plan(Create(new Timeframe(1, "d"), new Timeframe(1, "h")), model);
        var liveH = Assert.Single(entities, e => e.Id == "bar_1h_live" && e.Role == Role.Live);
        Assert.Equal("bar_1s_final_s", liveH.InputHint);
        var liveD = Assert.Single(entities, e => e.Id == "bar_1d_live" && e.Role == Role.Live);
        Assert.Equal("bar_1s_final_s", liveD.InputHint);
    }

    [Fact]
    public void Plan_Month_Windows_Use_Hub()
    {
        var model = new EntityModel { EntityType = typeof(Source) };
        var entities = DerivationPlanner.Plan(Create(new Timeframe(1, "mo")), model);
        var live = Assert.Single(entities, e => e.Id == "bar_1mo_live" && e.Role == Role.Live);
        Assert.Equal("bar_1s_final_s", live.InputHint);
    }

    [Fact]
    public void Plan_Months_Array_Uses_Hub()
    {
        var model = new EntityModel { EntityType = typeof(Source) };
        var entities = DerivationPlanner.Plan(Create(new Timeframe(1, "mo"), new Timeframe(12, "mo")), model);
        var live1 = Assert.Single(entities, e => e.Id == "bar_1mo_live" && e.Role == Role.Live);
        Assert.Equal("bar_1s_final_s", live1.InputHint);
        var live12 = Assert.Single(entities, e => e.Id == "bar_12mo_live" && e.Role == Role.Live);
        Assert.Equal("bar_1s_final_s", live12.InputHint);
    }

    [Fact]
    public void Plan_WhenEmpty_Adds_Fill_Entity()
    {
        var model = new EntityModel { EntityType = typeof(Source) };
        var entities = DerivationPlanner.Plan(Create(new Timeframe(1, "m")), model, true);
        Assert.Contains(entities, e => e.Id == "bar_1m_fill" && e.Role == Role.Fill);
    }
}
