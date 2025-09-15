using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Query.Pipeline;
using System;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Pipeline;

public class ExpressionAnalysisResultMonthTests
{
    [KsqlTopic("bar")]
    private class Source { }

    [Fact]
    public void Metadata_Uses_Hub_For_Month_Window()
    {
        var res = new ExpressionAnalysisResult
        {
            PocoType = typeof(Source),
            TimeKey = "Ts",
            BasedOnOpen = "Open",
            BasedOnClose = "Close",
            BasedOnDayKey = "Day"
        };
        res.BasedOnJoinKeys.Add("Id");
        res.Windows.Add("1mo");
        var md = res.ToMetadata();
        Assert.Equal("bar_1s_final_s", md.GetProperty<string>("input/1moLive"));
    }
}
