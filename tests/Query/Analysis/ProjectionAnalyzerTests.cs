using System;
using System.Linq;
using System.Linq.Expressions;
using Kafka.Ksql.Linq;
using Kafka.Ksql.Linq.Query.Analysis;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Analysis;

public class ProjectionAnalyzerTests
{
    [Fact]
    public void Validate_NoWindowStart_Throws()
    {
        Expression<Func<IGrouping<int, int>, object>> expr = g => new { Count = g.Count() };
        var ex = Assert.Throws<InvalidOperationException>(() => ProjectionAnalyzer.Validate(expr));
        Assert.Contains("WindowStart() projection required for windowed queries", ex.Message);
    }

    [Fact]
    public void Validate_MultipleWindowStart_Throws()
    {
        Expression<Func<IGrouping<int, int>, object>> expr = g => new { Start1 = g.WindowStart(), Start2 = g.WindowStart() };
        var ex = Assert.Throws<InvalidOperationException>(() => ProjectionAnalyzer.Validate(expr));
        Assert.Contains("Windowed query requires exactly one WindowStart() in projection.", ex.Message);
    }
}
