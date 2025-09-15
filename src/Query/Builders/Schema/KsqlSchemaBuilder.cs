using Kafka.Ksql.Linq.Core.Models;
using Kafka.Ksql.Linq.Query.Schema;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Kafka.Ksql.Linq.Query.Builders.Schema;

/// <summary>
/// Build KSQL column schema definitions from a select projection.
/// </summary>
public static class KsqlSchemaBuilder
{
    public static List<KsqlColumn> BuildSchema(LambdaExpression selectExpression)
    {
        if (selectExpression == null) throw new ArgumentNullException(nameof(selectExpression));

        if (selectExpression.Body is not NewExpression newExpr)
            throw new NotSupportedException("Only object initializer projections are supported.");

        var targetType = newExpr.Type;
        var props = targetType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .OrderBy(p => p.MetadataToken);

        var columns = new List<KsqlColumn>();

        foreach (var prop in props)
        {
            var meta = PropertyMeta.FromProperty(prop);
            if (meta.Attributes.Any(a => a.GetType().Name == "KsqlIgnoreAttribute"))
                continue;

            var ksqlType = KsqlTypeMapping.MapToKsqlType(meta.PropertyType, meta.PropertyInfo, meta.Precision, meta.Scale);
            var isKey = meta.Attributes.Any(a => a.GetType().Name == "KsqlKeyAttribute");
            columns.Add(new KsqlColumn(meta.Name, ksqlType, isKey));
        }

        return columns;
    }
}
