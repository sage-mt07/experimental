using Kafka.Ksql.Linq.Query.Builders;
using System;
using System.Linq;
using System.Linq.Expressions;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Builders.Visitors;

public class SelectExpressionVisitorDtoOrderTests
{
    private class DtoValid
    {
        public int Id { get; set; }
        public string Type { get; set; } = string.Empty;
    }

    private class DtoInvalid
    {
        public string Type { get; set; } = string.Empty;
        public int Id { get; set; }
    }

    private class GroupKey
    {
        public int Id { get; set; }
        public string Type { get; set; } = string.Empty;
    }

    [Fact]
    public void VisitNew_GroupKeyOrderMismatch_Throws()
    {
        Expression<Func<TestEntity, GroupKey>> group = e => new GroupKey { Id = e.Id, Type = e.Type };
        var builder = new GroupByClauseBuilder();
        builder.Build(group.Body);
        Expression<Func<IGrouping<GroupKey, TestEntity>, DtoInvalid>> select = g => new DtoInvalid { Type = g.Key.Type, Id = g.Key.Id };

        var visitor = new SelectExpressionVisitor();
        var ex = Assert.Throws<InvalidOperationException>(() => visitor.Visit(select.Body));
        Assert.Contains("GroupBy keys", ex.Message);
    }

    [Fact]
    public void VisitNew_GroupKeyOrderMismatch_MessageContainsHint()
    {
        Expression<Func<TestEntity, GroupKey>> group = e => new GroupKey { Id = e.Id, Type = e.Type };
        var builder = new GroupByClauseBuilder();
        builder.Build(group.Body);
        Expression<Func<IGrouping<GroupKey, TestEntity>, DtoInvalid>> select = g => new DtoInvalid { Type = g.Key.Type, Id = g.Key.Id };

        var visitor = new SelectExpressionVisitor();
        var ex = Assert.Throws<InvalidOperationException>(() => visitor.Visit(select.Body));
        Assert.Contains("The order of GroupBy keys does not match the output DTO definition", ex.Message);
    }

    [Fact]
    public void VisitNew_GroupKeyOrderMatch_NoException()
    {
        Expression<Func<TestEntity, GroupKey>> group = e => new GroupKey { Id = e.Id, Type = e.Type };
        var builder = new GroupByClauseBuilder();
        builder.Build(group.Body);
        Expression<Func<IGrouping<GroupKey, TestEntity>, DtoValid>> select = g => new DtoValid { Id = g.Key.Id, Type = g.Key.Type };

        var visitor = new SelectExpressionVisitor();
        visitor.Visit(select.Body);
        var result = visitor.GetResult();
        Assert.NotNull(result);
    }
}

