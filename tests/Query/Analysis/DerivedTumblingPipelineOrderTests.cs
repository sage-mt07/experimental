using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq;
using Kafka.Ksql.Linq.Query.Analysis;
using Kafka.Ksql.Linq.Query.Dsl;
using Kafka.Ksql.Linq.Mapping;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Analysis;

public class DerivedTumblingPipelineOrderTests
{
    [KsqlTopic("foo")]
    public class TestSource
    {
        public int Id { get; set; }
    }

    [Fact]
    public async Task Emits_DDL_in_expected_order()
    {
        var qao = new TumblingQao {
            TimeKey = "Timestamp",
            Windows = new[] { new Timeframe(1,"m"), new Timeframe(5,"m") },
            Keys = new[] { "Id", "BucketStart" },
            Projection = new[] { "Id", "BucketStart", "KsqlTimeFrameClose" },
            PocoShape = new[] {
                new ColumnShape("Id", typeof(int), false),
                new ColumnShape("BucketStart", typeof(long), false),
                new ColumnShape("KsqlTimeFrameClose", typeof(double), false)
            },
            BasedOn = new BasedOnSpec(new[] { "Id", "BucketStart" }, string.Empty, "KsqlTimeFrameClose", string.Empty),
            WeekAnchor = DayOfWeek.Monday
        };
        var baseModel = new EntityModel { EntityType = typeof(TestSource) };
        var model = new KsqlQueryModel { SourceTypes = new[] { typeof(TestSource) }, Windows = { "1m", "5m" } };

        var order = new List<string>();
        Task<KsqlDbResponse> Exec(EntityModel _, string sql) { order.Add(sql); return Task.FromResult(new KsqlDbResponse(true, string.Empty)); }

        // Enable WhenEmpty path to include HB per timeframe
        model.WhenEmptyFiller = (System.Linq.Expressions.Expression<System.Func<int,int,int>>)((a,b) => a);
        _ = await DerivedTumblingPipeline.RunAsync(qao, baseModel, model, Exec,
            _ => typeof(object), new MappingRegistry(), new(), NullLoggerFactory.Instance.CreateLogger("test"));

        // Validate relative order of key DDLs; additional DDLs (Fill/Prev etc.) may exist
        int idx(string prefix) => order.FindIndex(s => s.StartsWith(prefix, StringComparison.Ordinal));
        var iTable1s = idx("CREATE TABLE foo_1s_final");
        var iStream1s = idx("CREATE STREAM foo_1s_final_s");
        var iHb1s = idx("CREATE TABLE foo_hb_1s");
        var iLive1m = idx("CREATE TABLE foo_1m_live");
        var iLive5m = idx("CREATE TABLE foo_5m_live");
        Assert.True(iTable1s >= 0 && iStream1s > iTable1s && iHb1s > iStream1s && iLive1m > iHb1s && iLive5m > iLive1m, string.Join("\n", order));
    }
}


