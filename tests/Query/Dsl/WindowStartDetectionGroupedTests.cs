using System;
using System.Linq;
using Kafka.Ksql.Linq;
using Kafka.Ksql.Linq.Query.Dsl;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Dsl;

public class WindowStartDetectionGroupedTests
{
    private class Tick
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime TimestampUtc { get; set; }
        public decimal Bid { get; set; }
    }

    private class Bar
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime BucketStart { get; set; }
        public decimal Open { get; set; }
    }

    [Fact]
    public void GroupedSelect_WithWindowStart_SetsBucketColumnName()
    {
        var model = new KsqlQueryRoot()
            .From<Tick>()
            .Tumbling(r => r.TimestampUtc, new Windows { Minutes = new[] { 1 } })
            .GroupBy(r => new { r.Broker, r.Symbol })
            .Select(g => new Bar
            {
                Broker = g.Key.Broker,
                Symbol = g.Key.Symbol,
                BucketStart = g.WindowStart(),
                Open = g.Min(x => x.Bid)
            })
            .Build();

        Assert.Equal("BucketStart", model.BucketColumnName);
    }
}

