using System;
using System.Linq;
using System.Linq.Expressions;
using Kafka.Ksql.Linq.Query.Builders;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Pipeline;

public class WindowStartDetectionVisitorTests
{
    [Fact]
    public void Detects_WindowStart_Alias()
    {
        Expression<Func<IGrouping<int, int>, object>> expr = g => new { Start = g.WindowStart() };
        var visitor = new WindowStartDetectionVisitor();
        visitor.Visit(expr.Body);
        Assert.Equal(1, visitor.Count);
        Assert.Equal("Start", visitor.ColumnName);
    }
}
