using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Query.Analysis;
using Kafka.Ksql.Linq.Query.Builders;
using Kafka.Ksql.Linq.Query.Builders.Core;
using Kafka.Ksql.Linq.Query.Dsl;
using Kafka.Ksql.Linq.Query.Pipeline;
using System;
using System.Linq;
using System.Linq.Expressions;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Builders;

public class RollupBuilderTests
{
    [KsqlTopic("trade_raw_filtered")]
    private class Rate
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime Timestamp { get; set; }
        public decimal Open { get; set; }
        public decimal High { get; set; }
        public decimal Low { get; set; }
        public decimal Close { get; set; }
    }

    private class MarketSchedule
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime Open { get; set; }
        public DateTime Close { get; set; }
        public DateTime MarketDate { get; set; }
    }

    private static QueryMetadata BuildMetadata()
    {
        Expression expr = ((Expression<Func<KsqlQueryable<Rate>, object>>)(q => q
            .TimeFrame<MarketSchedule>(
                (r, s) =>
                    r.Broker == s.Broker &&
                    r.Symbol == s.Symbol &&
                    s.Open <= r.Timestamp &&
                    r.Timestamp < s.Close,
                s => s.MarketDate)
            .Tumbling(r => r.Timestamp, new Windows { Minutes = new[] { 1, 5 } }, 10, null)
            .GroupBy(r => new { r.Broker, r.Symbol })
            .Select(g => new { g.Key.Broker, g.Key.Symbol, BucketStart = g.WindowStart() }))).Body;
        var analysis = Analyze(expr);
        return analysis.ToMetadata();
    }

    private static ExpressionAnalysisResult Analyze(Expression expr)
    {
        var visitor = new MethodCallCollectorVisitor();
        visitor.Visit(expr);
        var result = visitor.Result;
        var selectCall = result.MethodCalls.FirstOrDefault(mc => mc.Method.Name == "Select");
        if (selectCall != null && selectCall.Arguments.Count > 1)
        {
            Expression? projExpr = selectCall.Arguments[1] switch
            {
                LambdaExpression le => le.Body,
                UnaryExpression ue when ue.Operand is LambdaExpression le => le.Body,
                ConstantExpression ce when ce.Value is LambdaExpression le => le.Body,
                _ => null
            };
            if (projExpr != null)
            {
                var ws = new WindowStartDetectionVisitor();
                ws.Visit(projExpr);
                result.BucketColumnName = ws.ColumnName;
            }
        }
        result.PocoType = typeof(Rate);
        if (result.Windows.Count == 0)
            throw new InvalidOperationException("Tumbling windows are required");
        if (result.TimeKey == null)
            throw new InvalidOperationException("Time key is required");
        return result;
    }

    [Fact]
    public void Live_1m_IsTable_EmitChanges_NoSync()
    {
        var md = BuildMetadata();
        var sql = LiveBuilder.Build(md, "1m");
        Assert.Contains("TABLE trade_raw_filtered_1s_final_s WINDOW TUMBLING(1m)", sql);
        Assert.DoesNotContain("SYNC", sql);
    }

    [Fact]
    public void Live_5m_RollsUp_From_1mLive()
    {
        var md = BuildMetadata();
        var sql = LiveBuilder.Build(md, "5m");
        Assert.Contains("TABLE trade_raw_filtered_1s_final_s WINDOW TUMBLING(5m)", sql);
        Assert.DoesNotContain("SYNC", sql);
    }

    [Fact]
    public void Live_Uses_Window_NoFinal()
    {
        var md = BuildMetadata();
        var sql = LiveBuilder.Build(md, "5m");
        Assert.Contains("TABLE trade_raw_filtered_1s_final_s WINDOW TUMBLING(5m)", sql);
        Assert.DoesNotContain("EMIT FINAL", sql);
        Assert.DoesNotContain("SYNC", sql);
        Assert.DoesNotContain("COMPOSE(", sql);
        var sql1 = LiveBuilder.Build(md, "1m");
        Assert.Contains("TABLE trade_raw_filtered_1s_final_s WINDOW TUMBLING(1m)", sql1);
        Assert.DoesNotContain("EMIT FINAL", sql1);
        Assert.DoesNotContain("SYNC", sql1);
        Assert.DoesNotContain("COMPOSE(", sql1);
    }

    [Fact]
    public void Builders_Expand_TimeFrame_Join_And_Boundary_For_Live()
    {
        var md = BuildMetadata();
        var live = LiveBuilder.Build(md, "1m");
        Assert.Contains("JOIN", live);
        Assert.Contains("<=", live);
        Assert.Contains("<", live);
    }

    [Fact]
    public void RoleSpec_Table()
    {
        var specLive = RoleTraits.For(Role.Live);
        Assert.True(specLive.Window);
        Assert.Equal("CHANGES", specLive.Emit);

        var specFinal = RoleTraits.For(Role.Final);
        Assert.True(specFinal.Window);
        Assert.Equal("FINAL", specFinal.Emit);
    }
}
