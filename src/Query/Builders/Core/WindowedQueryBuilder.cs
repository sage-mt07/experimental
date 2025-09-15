using Kafka.Ksql.Linq.Query.Analysis;
using Kafka.Ksql.Linq.Query.Builders.Utils;
using Kafka.Ksql.Linq.Query.Pipeline;
using System.Text;

namespace Kafka.Ksql.Linq.Query.Builders.Core;

internal static class WindowedQueryBuilder
{
    public static string Build(Role role, string timeframe, QueryMetadata md)
    {
        var tfStr = timeframe;
        var spec = RoleTraits.For(role);
        var input = role switch
        {
            Role.Live => QueryBuilderUtils.ResolveInput(md.GetProperty<string>($"input/{tfStr}Live")),
            Role.Final => QueryBuilderUtils.ResolveInput(md.GetProperty<string>($"input/{tfStr}Final")),
            _ => string.Empty
        };
        var sb = new StringBuilder();
        if (role == Role.Live || role == Role.Final)
            sb.Append($"TABLE {input}");
        if (spec.Window)
        {
            var grace = md.GetProperty<int?>($"grace/{tfStr}");
            sb.Append(' ').Append(QueryBuilderUtils.ApplyWindowTumbling(tfStr, grace));
        }
        if (spec.Emit != null)
            sb.Append(' ').Append($"EMIT {spec.Emit}");
        sb.Append(' ').Append(QueryBuilderUtils.ApplyTimeFrame(md));
        var sql = sb.ToString().Trim();
        if (role == Role.Final && sql.Contains("COMPOSE(", System.StringComparison.OrdinalIgnoreCase))
            throw new System.InvalidOperationException("Final SQL should not include COMPOSE()");
        return sql;
    }
}
