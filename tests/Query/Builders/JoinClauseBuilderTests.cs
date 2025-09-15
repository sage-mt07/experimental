using Kafka.Ksql.Linq.Query.Builders;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Builders;

public class JoinClauseBuilderTests
{

    [Fact]
    public void Build_InnerJoin_ReturnsJoinSql()
    {
        IQueryable<TestEntity> outer = new List<TestEntity>().AsQueryable();
        IQueryable<ChildEntity> inner = new List<ChildEntity>().AsQueryable();

        var join = Queryable.Join(outer,
            inner,
            o => o.Id,
            i => i.ParentId,
            (o, i) => new { o.Id, i.Name });

        var builder = new JoinClauseBuilder();
        var sql = builder.Build(join.Expression);

        const string expected = "SELECT o.Id AS Id, i.Name AS Name FROM TestEntity o JOIN ChildEntity i ON o.Id = i.ParentId";
        Assert.Equal(expected, sql);
    }

    [Fact]
    public void Build_CompositeKeyJoin_GeneratesAndConditions()
    {
        IQueryable<TestEntity> outer = new List<TestEntity>().AsQueryable();
        IQueryable<ChildEntity> inner = new List<ChildEntity>().AsQueryable();

        var join = Queryable.Join(outer,
            inner,
            o => new { Id = o.Id, Type = o.Type },
            i => new { Id = i.ParentId, Type = i.Name },
            (o, i) => new { o.Id, i.Name });

        var builder = new JoinClauseBuilder();
        var sql = builder.Build(join.Expression);

        const string expected = "SELECT o.Id AS Id, i.Name AS Name FROM TestEntity o JOIN ChildEntity i ON o.Id = i.ParentId AND o.Type = i.Name";
        Assert.Equal(expected, sql);
    }

    [Fact]
    public void Build_JoinWithThirdTable_Throws()
    {
        IQueryable<TestEntity> t1 = new List<TestEntity>().AsQueryable();
        IQueryable<ChildEntity> t2 = new List<ChildEntity>().AsQueryable();
        IQueryable<GrandChildEntity> t3 = new List<GrandChildEntity>().AsQueryable();

        var join = Queryable.Join(t1, t2, o => o.Id, i => i.ParentId, (o, i) => new { o, i })
                     .Join(t3, x => x.o.Id, g => g.ChildId, (x, g) => new { x.o, x.i, g });

        var builder = new JoinClauseBuilder();

        Assert.Throws<InvalidOperationException>(() => builder.Build(join.Expression));
    }
}
