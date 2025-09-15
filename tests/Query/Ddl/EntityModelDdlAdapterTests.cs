using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Query.Abstractions;
using Kafka.Ksql.Linq.Query.Ddl;
using System.Linq;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Ddl;

public class EntityModelDdlAdapterTests
{
    private class Sample
    {
        public int A { get; set; }
        public int B { get; set; }
        public int C { get; set; }
    }

    [Fact]
    public void Ddl_Preserves_KeyOrder_And_ProjectionOrder()
    {
        var model = new EntityModel { EntityType = typeof(Sample) };
        model.AdditionalSettings["keys"] = new[] { "B", "A" };
        model.AdditionalSettings["projection"] = new[] { "B", "A", "C" };
        var adapter = new EntityModelDdlAdapter(model);
        var schema = adapter.GetSchema();
        Assert.Equal(new[] { "B", "A", "C" }, schema.Columns.Select(c => c.Name));
        Assert.True(schema.Columns[0].IsKey);
        Assert.True(schema.Columns[1].IsKey);
        Assert.False(schema.Columns[2].IsKey);
    }

    [Fact]
    public void Rocks_Created_For_TableOnly_StreamNone()
    {
        var tableModel = new EntityModel { EntityType = typeof(Sample) };
        tableModel.SetStreamTableType(StreamTableType.Table);
        var streamModel = new EntityModel { EntityType = typeof(Sample) };
        streamModel.SetStreamTableType(StreamTableType.Stream);
        var tableSchema = new EntityModelDdlAdapter(tableModel).GetSchema();
        var streamSchema = new EntityModelDdlAdapter(streamModel).GetSchema();
        Assert.Equal(DdlObjectType.Table, tableSchema.ObjectType);
        Assert.Equal(DdlObjectType.Stream, streamSchema.ObjectType);
    }
}
