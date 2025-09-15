using Kafka.Ksql.Linq.Query.Builders;
using Kafka.Ksql.Linq.Query.Dsl;
using System;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Builders;

public class JoinWithinBuilderTests
{
    private class L { public int Id { get; set; } public int FK { get; set; } }
    private class R { public int Id { get; set; } }

    [Fact]
    public void Join_With_Explicit_Within_TimeSpan_WritesSeconds()
    {
        var model = new KsqlQueryRoot()
            .From<L>()
            .Join<R>((o, i) => o.FK == i.Id)
            .Within(TimeSpan.FromSeconds(42))
            .Select((o, i) => new { o.Id })
            .Build();

        var sql = KsqlCreateStatementBuilder.Build("t", model);
        Assert.Contains("WITHIN 42 SECONDS", sql);
    }

    [Fact]
    public void Join_Without_Within_Uses_Default_300s()
    {
        var model = new KsqlQueryRoot()
            .From<L>()
            .Join<R>((o, i) => o.FK == i.Id)
            .Select((o, i) => new { o.Id })
            .Build();

        var sql = KsqlCreateStatementBuilder.Build("t", model);
        Assert.Contains("WITHIN 300 SECONDS", sql);
    }

    [Fact]
    public void Join_Without_Within_And_Default_Forbidden_Throws()
    {
        var query = new KsqlQueryRoot()
            .From<L>()
            .Join<R>((o, i) => o.FK == i.Id)
            .RequireExplicitWithin()
            .Select((o, i) => new { o.Id });

        var model = query.Build();
        Assert.Throws<InvalidOperationException>(() => KsqlCreateStatementBuilder.Build("t", model));
    }

    [Fact]
    public void Join_On_Unqualified_Column_Throws()
    {
        // Build a condition that cannot be tied to either parameter cleanly
        int external = 1;
        var query = new KsqlQueryRoot()
            .From<L>()
            .Join<R>((o, i) => o.FK == external) // RHS not from i-parameter
            .Select((o, i) => new { o.Id });

        var model = query.Build();
        Assert.Throws<InvalidOperationException>(() => KsqlCreateStatementBuilder.Build("t", model));
    }
}

