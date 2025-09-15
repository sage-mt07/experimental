using System;
using Kafka.Ksql.Linq.Core.Attributes;

namespace Examples.Contracts;

[KsqlTopic("deduprates")]
public class DedupRateRecord
{
    [KsqlKey(order: 0)]
    public string Broker { get; set; } = string.Empty;

    [KsqlKey(order: 1)]
    public string Symbol { get; set; } = string.Empty;

    [KsqlTimestamp]
    public DateTime Ts { get; set; }

    [KsqlDecimal(precision: 18, scale: 4)]
    public decimal Bid { get; set; }
}

