using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Core.Modeling;
using Kafka.Ksql.Linq.Query.Abstractions;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.ModelBuilderTests;

public class TableAttributeTests
{
    [KsqlTable]
    private class TableEntity
    {
        [KsqlKey]
        public int Id { get; set; }
    }

    private class KeyedEntity
    {
        [KsqlKey]
        public int Id { get; set; }
    }

    [Fact]
    public void Attribute_Configures_Table()
    {
        var builder = new ModelBuilder();
        builder.Entity<TableEntity>();
        var model = builder.GetEntityModel<TableEntity>()!;
        Assert.Equal(StreamTableType.Table, model.StreamTableType);
    }

    [Fact]
    public void Keyed_Defaults_To_Stream()
    {
        var builder = new ModelBuilder();
        builder.Entity<KeyedEntity>();
        var model = builder.GetEntityModel<KeyedEntity>()!;
        Assert.Equal(StreamTableType.Stream, model.StreamTableType);
    }
}
