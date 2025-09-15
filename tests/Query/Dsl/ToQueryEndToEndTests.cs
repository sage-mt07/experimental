using Kafka.Ksql.Linq;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Core.Modeling;
using Kafka.Ksql.Linq.Query.Dsl;
using Kafka.Ksql.Linq.Query.Builders;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Dsl;

public class ToQueryEndToEndTests
{
    [KsqlTopic("deduprates")]
    private class DeDupRate
    {
        [KsqlKey] public string Broker { get; set; } = string.Empty;
        [KsqlKey] public string Symbol { get; set; } = string.Empty;
        [KsqlTimestamp] public DateTime Timestamp { get; set; }
        public double Bid { get; set; }
    }

    private class MarketSchedule
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime Open { get; set; }
        public DateTime Close { get; set; }
        public DateTime MarketDate { get; set; }
    }

    [KsqlTable]
    private class Rate
    {
        [KsqlKey] public string Broker { get; set; } = string.Empty;
        [KsqlKey] public string Symbol { get; set; } = string.Empty;
        [KsqlKey] public DateTime BucketStart { get; set; }
        public double Open { get; set; }
        public double High { get; set; }
        public double Low { get; set; }
        public double Close { get; set; }
    }

    [KsqlTable]
    private class BidStats
    {
        [KsqlKey] public string Broker { get; set; } = string.Empty;
        [KsqlKey] public string Symbol { get; set; } = string.Empty;
        [KsqlKey] public DateTime BucketStart { get; set; }
        public long Count { get; set; }
        public double Avg { get; set; }
    }

    private class DummyContext : IKsqlContext
    {
        public IEntitySet<T> Set<T>() where T : class => throw new NotImplementedException();
        public object GetEventSet(Type entityType) => throw new NotImplementedException();
        public Dictionary<Type, EntityModel> GetEntityModels() => new();
        public void Dispose() { }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private class RateSet : EventSet<Rate>
    {
        public RateSet(EntityModel model) : base(new DummyContext(), model) { }
        protected override Task SendEntityAsync(Rate entity, Dictionary<string, string>? headers, CancellationToken cancellationToken) => Task.CompletedTask;
        public override async IAsyncEnumerator<Rate> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            await Task.CompletedTask;
            yield break;
        }
    }

    private class BidStatsSet : EventSet<BidStats>
    {
        public BidStatsSet(EntityModel model) : base(new DummyContext(), model) { }
        protected override Task SendEntityAsync(BidStats entity, Dictionary<string, string>? headers, CancellationToken cancellationToken) => Task.CompletedTask;
        public override async IAsyncEnumerator<BidStats> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            await Task.CompletedTask;
            yield break;
        }
    }

    [KsqlTable]
    private class SingleKeyRate
    {
        [KsqlKey] public string Broker { get; set; } = string.Empty;
        [KsqlKey] public DateTime BucketStart { get; set; }
        public double Open { get; set; }
        public double High { get; set; }
        public double Low { get; set; }
        public double Close { get; set; }
    }

    private class SingleKeyRateSet : EventSet<SingleKeyRate>
    {
        public SingleKeyRateSet(EntityModel model) : base(new DummyContext(), model) { }
        protected override Task SendEntityAsync(SingleKeyRate entity, Dictionary<string, string>? headers, CancellationToken cancellationToken) => Task.CompletedTask;
        public override async IAsyncEnumerator<SingleKeyRate> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            await Task.CompletedTask;
            yield break;
        }
    }

    [Fact]
    public void Generates_expected_ddls()
    {
        var mb = new ModelBuilder();
        mb.Entity<Rate>();
        var model = mb.GetEntityModel<Rate>()!;
        var set = new RateSet(model);

        set.ToQuery(q => q
            .From<DeDupRate>()
            .TimeFrame<MarketSchedule>((r, s) =>
                r.Broker == s.Broker &&
                r.Symbol == s.Symbol &&
                s.Open <= r.Timestamp &&
                r.Timestamp < s.Close,
                dayKey: s => s.MarketDate)
            .Tumbling(r => r.Timestamp, new Windows
            {
                Minutes = new[]{5,15,30},
                Hours = new[]{1,4,8},
                Days = new[]{1,7},
                Months = new[]{1,12}
            }, 1, TimeSpan.FromMinutes(2))
            .GroupBy(r => new { r.Broker, r.Symbol })
            .Select(g => new Rate
            {
                Broker = g.Key.Broker,
                Symbol = g.Key.Symbol,
                BucketStart = g.WindowStart(),
                Open = g.EarliestByOffset(x => x.Bid),
                High = g.Max(x => x.Bid),
                Low = g.Min(x => x.Bid),
                Close = g.LatestByOffset(x => x.Bid)
            })
        );

        var tableSql = KsqlCreateWindowedStatementBuilder.Build(
            "rate_1s_final",
            model.QueryModel!,
            "1s",
            "EMIT FINAL",
            "deduprates_1s_final_s");

        var streamModel = model.QueryModel!.Clone();
        streamModel.Windows.Clear();
        streamModel.GroupByExpression = null;
        streamModel.SelectProjection = (Expression<Func<Rate, Rate>>)(r => r);
        var streamSql = KsqlCreateStatementBuilder.Build(
            "rate_1s_final_s",
            streamModel,
            null,
            null,
            _ => "rate_1s_final");

        const string expectedTable = "CREATE TABLE rate_1s_final WITH (KAFKA_TOPIC='rate_1s_final', KEY_FORMAT='AVRO', VALUE_FORMAT='AVRO') AS\nSELECT BROKER AS Broker, SYMBOL AS Symbol, WINDOWSTART AS BucketStart, EARLIEST_BY_OFFSET(Bid) AS Open, MAX(Bid) AS High, MIN(Bid) AS Low, LATEST_BY_OFFSET(Bid) AS Close\nFROM deduprates_1s_final_s o WINDOW TUMBLING (SIZE 1 MINUTES)\nGROUP BY BROKER, SYMBOL\nEMIT FINAL;";
        const string expectedStream = "CREATE STREAM rate_1s_final_s WITH (KAFKA_TOPIC='rate_1s_final_s', KEY_FORMAT='AVRO', VALUE_FORMAT='AVRO') AS\nSELECT *\nFROM rate_1s_final o\nEMIT CHANGES;";

        Kafka.Ksql.Linq.Tests.Utils.SqlAssert.EqualNormalized(expectedTable, tableSql);
        Kafka.Ksql.Linq.Tests.Utils.SqlAssert.EqualNormalized(expectedStream, streamSql);
    }

    [Fact]
    public void Generates_count_and_avg_ddls()
    {
        var mb = new ModelBuilder();
        mb.Entity<BidStats>();
        var model = mb.GetEntityModel<BidStats>()!;
        var set = new BidStatsSet(model);

        set.ToQuery(q => q
            .From<DeDupRate>()
            .TimeFrame<MarketSchedule>((r, s) =>
                r.Broker == s.Broker &&
                r.Symbol == s.Symbol &&
                s.Open <= r.Timestamp &&
                r.Timestamp < s.Close,
                dayKey: s => s.MarketDate)
            .Tumbling(r => r.Timestamp, new Windows
            {
                Minutes = new[]{5,15,30},
                Hours = new[]{1,4,8},
                Days = new[]{1,7},
                Months = new[]{1,12}
            }, 1, TimeSpan.FromMinutes(2))
            .GroupBy(r => new { r.Broker, r.Symbol })
            .Select(g => new BidStats
            {
                Broker = g.Key.Broker,
                Symbol = g.Key.Symbol,
                BucketStart = g.WindowStart(),
                Count = g.Count(),
                Avg = g.Average(x => x.Bid)
            })
        );

        var tableSql = KsqlCreateWindowedStatementBuilder.Build(
            "bidstats_1s_final",
            model.QueryModel!,
            "1s",
            "EMIT FINAL",
            "deduprates_1s_final_s");

        var streamModel = model.QueryModel!.Clone();
        streamModel.Windows.Clear();
        streamModel.GroupByExpression = null;
        streamModel.SelectProjection = (Expression<Func<BidStats, BidStats>>)(r => r);
        var streamSql = KsqlCreateStatementBuilder.Build(
            "bidstats_1s_final_s",
            streamModel,
            null,
            null,
            _ => "bidstats_1s_final");

        const string expectedTable = "CREATE TABLE bidstats_1s_final WITH (KAFKA_TOPIC='bidstats_1s_final', KEY_FORMAT='AVRO', VALUE_FORMAT='AVRO') AS\nSELECT BROKER AS Broker, SYMBOL AS Symbol, WINDOWSTART AS BucketStart, COUNT(*) AS Count, AVG(Bid) AS Avg\nFROM deduprates_1s_final_s o WINDOW TUMBLING (SIZE 1 MINUTES)\nGROUP BY BROKER, SYMBOL\nEMIT FINAL;";
        const string expectedStream = "CREATE STREAM bidstats_1s_final_s WITH (KAFKA_TOPIC='bidstats_1s_final_s', KEY_FORMAT='AVRO', VALUE_FORMAT='AVRO') AS\nSELECT *\nFROM bidstats_1s_final o\nEMIT CHANGES;";

        Kafka.Ksql.Linq.Tests.Utils.SqlAssert.EqualNormalized(expectedTable, tableSql);
        Kafka.Ksql.Linq.Tests.Utils.SqlAssert.EqualNormalized(expectedStream, streamSql);
    }

    [Fact]
    public void Generates_single_key_ddls()
    {
        var mb = new ModelBuilder();
        mb.Entity<SingleKeyRate>();
        var model = mb.GetEntityModel<SingleKeyRate>()!;
        var set = new SingleKeyRateSet(model);

        set.ToQuery(q => q
            .From<DeDupRate>()
            .TimeFrame<MarketSchedule>((r, s) =>
                r.Broker == s.Broker &&
                r.Symbol == s.Symbol &&
                s.Open <= r.Timestamp &&
                r.Timestamp < s.Close,
                dayKey: s => s.MarketDate)
            .Tumbling(r => r.Timestamp, new Windows
            {
                Minutes = new[]{5,15,30},
                Hours = new[]{1,4,8},
                Days = new[]{1,7},
                Months = new[]{1,12}
            }, 1, TimeSpan.FromMinutes(2))
            .GroupBy(r => r.Broker)
            .Select(g => new SingleKeyRate
            {
                Broker = g.Key,
                BucketStart = g.WindowStart(),
                Open = g.EarliestByOffset(x => x.Bid),
                High = g.Max(x => x.Bid),
                Low = g.Min(x => x.Bid),
                Close = g.LatestByOffset(x => x.Bid)
            })
        );

        var tableSql = KsqlCreateWindowedStatementBuilder.Build(
            "rate_broker_1s_final",
            model.QueryModel!,
            "1s",
            "EMIT FINAL",
            "deduprates_1s_final_s");

        var streamModel = model.QueryModel!.Clone();
        streamModel.Windows.Clear();
        streamModel.GroupByExpression = null;
        streamModel.SelectProjection = (Expression<Func<SingleKeyRate, SingleKeyRate>>)(r => r);
        var streamSql = KsqlCreateStatementBuilder.Build(
            "rate_broker_1s_final_s",
            streamModel,
            null,
            null,
            _ => "rate_broker_1s_final");

        const string expectedTable = "CREATE TABLE rate_broker_1s_final WITH (KAFKA_TOPIC='rate_broker_1s_final', KEY_FORMAT='AVRO', VALUE_FORMAT='AVRO') AS\nSELECT BROKER AS BROKER, WINDOWSTART AS BucketStart, EARLIEST_BY_OFFSET(Bid) AS Open, MAX(Bid) AS High, MIN(Bid) AS Low, LATEST_BY_OFFSET(Bid) AS Close\nFROM deduprates_1s_final_s o WINDOW TUMBLING (SIZE 1 MINUTES)\nGROUP BY BROKER\nEMIT FINAL;";
        const string expectedStream = "CREATE STREAM rate_broker_1s_final_s WITH (KAFKA_TOPIC='rate_broker_1s_final_s', KEY_FORMAT='AVRO', VALUE_FORMAT='AVRO') AS\nSELECT *\nFROM rate_broker_1s_final o\nEMIT CHANGES;";

        Kafka.Ksql.Linq.Tests.Utils.SqlAssert.EqualNormalized(expectedTable, tableSql);
        Kafka.Ksql.Linq.Tests.Utils.SqlAssert.EqualNormalized(expectedStream, streamSql);

    }

    private static string NL(string s) => s.Replace("\r\n", "\n");
}
