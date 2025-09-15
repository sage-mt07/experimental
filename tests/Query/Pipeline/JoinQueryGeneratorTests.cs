using Kafka.Ksql.Linq.Query.Pipeline;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Pipeline;

public class JoinQueryGeneratorTests
{
    [Fact]
    public void GenerateTwoTableJoin_ReturnsJoinQuery()
    {
        Expression<Func<TestEntity, int>> outerKey = e => e.Id;
        Expression<Func<ChildEntity, int>> innerKey = c => c.ParentId;

        var generator = new JoinQueryGenerator();
        var sql = generator.GenerateTwoTableJoin(
            "TestEntity",
            "ChildEntity",
            outerKey,
            innerKey,
            resultSelector: null,
            whereCondition: null,
            isPullQuery: true);

        Assert.Contains("FROM TestEntity", sql);
        Assert.Contains("JOIN ChildEntity", sql);
        Assert.Contains("ON e.Id = c.ParentId", sql);
    }


    [Fact]
    public void GenerateLeftJoin_ReturnsLeftJoinQuery()
    {
        Expression<Func<TestEntity, int>> outerKey = e => e.Id;
        Expression<Func<ChildEntity, int>> innerKey = c => c.ParentId;

        var generator = new JoinQueryGenerator();
        var sql = generator.GenerateLeftJoin(
            "TestEntity",
            "ChildEntity",
            outerKey,
            innerKey,
            resultSelector: null,
            isPullQuery: true);

        Assert.Contains("LEFT JOIN ChildEntity", sql);
    }

    [Fact]
    public void GenerateTwoTableJoin_CompositeKeys_ReturnsJoinQuery()
    {
        Expression<Func<TestEntity, object>> outerKey = e => new { e.Id, e.Type };
        Expression<Func<ChildEntity, object>> innerKey = c => new { Id = c.ParentId, Type = c.Name };

        var generator = new JoinQueryGenerator();
        var sql = generator.GenerateTwoTableJoin(
            "TestEntity",
            "ChildEntity",
            outerKey,
            innerKey,
            resultSelector: null,
            whereCondition: null,
            isPullQuery: true);

        Assert.Contains("JOIN ChildEntity", sql);
        Assert.Contains("ON e.Id = c.ParentId AND e.Type = c.Name", sql);
    }

    [Fact]
    public void GenerateFromLinqJoin_ThirdTable_Throws()
    {
        IQueryable<TestEntity> t1 = new List<TestEntity>().AsQueryable();
        IQueryable<ChildEntity> t2 = new List<ChildEntity>().AsQueryable();
        IQueryable<GrandChildEntity> t3 = new List<GrandChildEntity>().AsQueryable();

        var expr = t1.Join(t2, o => o.Id, i => i.ParentId, (o, i) => new { o, i })
                      .Join(t3, x => x.o.Id, g => g.ChildId, (x, g) => new { x.o, x.i, g })
                      .Expression;

        var generator = new JoinQueryGenerator();

        var ex = Assert.Throws<InvalidOperationException>(() => generator.GenerateFromLinqJoin(expr));
        Assert.Contains("maximum", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}
