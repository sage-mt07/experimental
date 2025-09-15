using Kafka.Ksql.Linq.Query.Builders.Common;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Builders;

public class JoinLimitationEnforcerTests
{
    private class ExtraEntity { public int RefId { get; set; } }

    [Fact]
    public void ValidateJoinExpression_TooManyTables_Throws()
    {
        IQueryable<TestEntity> t1 = new List<TestEntity>().AsQueryable();
        IQueryable<ChildEntity> t2 = new List<ChildEntity>().AsQueryable();
        IQueryable<GrandChildEntity> t3 = new List<GrandChildEntity>().AsQueryable();

        var expr = t1.Join(t2, o => o.Id, i => i.ParentId, (o, i) => new { o, i })
                      .Join(t3, x => x.o.Id, g => g.ChildId, (x, g) => new { x.o, x.i, g })
                      .Expression;

        Assert.Throws<StreamProcessingException>(() => JoinLimitationEnforcer.ValidateJoinExpression(expr));
    }

    [Fact]
    public void ValidateJoinExpression_UnsupportedPattern_Throws()
    {
        IQueryable<TestEntity> t1 = new List<TestEntity>().AsQueryable();
        IQueryable<ChildEntity> t2 = new List<ChildEntity>().AsQueryable();

        var expr = t1.GroupJoin(t2, o => o.Id, i => i.ParentId, (o, g) => new { o, g }).Expression;

        Assert.Throws<StreamProcessingException>(() => JoinLimitationEnforcer.ValidateJoinExpression(expr));
    }

    [Fact]
    public void ValidateJoinExpression_ValidJoin_NoException()
    {
        IQueryable<TestEntity> t1 = new List<TestEntity>().AsQueryable();
        IQueryable<ChildEntity> t2 = new List<ChildEntity>().AsQueryable();

        var expr = t1.Join(t2, o => o.Id, i => i.ParentId, (o, i) => new { o, i }).Expression;

        JoinLimitationEnforcer.ValidateJoinExpression(expr);
    }
}
