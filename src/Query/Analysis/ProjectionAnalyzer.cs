using System;
using System.Linq.Expressions;
using Kafka.Ksql.Linq.Query.Builders;

namespace Kafka.Ksql.Linq.Query.Analysis;

internal static class ProjectionAnalyzer
{
    public static void Validate(LambdaExpression projection)
    {
        if (projection == null) throw new ArgumentNullException(nameof(projection));
        var visitor = new WindowStartDetectionVisitor();
        visitor.Visit(projection.Body);
        if (visitor.Count == 0)
            throw new InvalidOperationException("WindowStart() projection required for windowed queries");
        if (visitor.Count != 1)
            throw new InvalidOperationException("Windowed query requires exactly one WindowStart() in projection.");
    }
}
