using Kafka.Ksql.Linq.Query.Pipeline;
using System;
using System.Linq;

namespace Kafka.Ksql.Linq.Query.Builders.Utils;

internal static class QueryBuilderUtils
{
    public static string ResolveInput(string? hint) => hint ?? string.Empty;

    public static string ApplyTimeFrame(QueryMetadata md)
    {
        var joinKeys = md.GetProperty<string[]>("basedOn/joinKeys") ?? Array.Empty<string>();
        var openProp = md.GetProperty<string>("basedOn/openProp");
        var closeProp = md.GetProperty<string>("basedOn/closeProp");
        var timeKey = md.GetProperty<string>("timeKey");
        var openInc = md.GetProperty<bool?>("basedOn/openInclusive") ?? true;
        var closeInc = md.GetProperty<bool?>("basedOn/closeInclusive") ?? false;
        var openOp = openInc ? "<=" : "<";
        var closeOp = closeInc ? "<=" : "<";
        var join = string.Join(" AND ", joinKeys.Select(k => $"r.{k} = s.{k}"));
        return $"JOIN ON {join} AND s.{openProp} {openOp} r.{timeKey} AND r.{timeKey} {closeOp} s.{closeProp}"; 
    }

    public static string ApplyWindowTumbling(string timeframe, int? graceSeconds = null)
    {
        var grace = graceSeconds.HasValue ? $" GRACE PERIOD {graceSeconds.Value}s" : string.Empty;
        return $"WINDOW TUMBLING({timeframe}{grace})";
    }
}
