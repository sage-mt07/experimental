using System;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Ksql.Linq.Query.Pipeline;
internal record QueryAssemblyResult(
    string FinalQuery,
    QueryAssemblyContext Context,
    List<QueryPart> Parts,
    DateTime AssembledAt,
    bool IsValid)
{
    /// <summary>
    /// Create success result
    /// </summary>
    public static QueryAssemblyResult Success(string query, QueryAssemblyContext context, List<QueryPart> parts)
    {
        return new QueryAssemblyResult(query, context, parts, DateTime.UtcNow, true);
    }

    /// <summary>
    /// Create failure result
    /// </summary>
    public static QueryAssemblyResult Failure(string error, QueryAssemblyContext context)
    {
        return new QueryAssemblyResult(error, context, new List<QueryPart>(), DateTime.UtcNow, false);
    }

    /// <summary>
    /// Assembly statistics information
    /// </summary>
    public QueryAssemblyStats GetStats()
    {
        return new QueryAssemblyStats(
            TotalParts: Parts.Count,
            RequiredParts: Parts.Count(p => p.IsRequired),
            OptionalParts: Parts.Count(p => !p.IsRequired),
            QueryLength: FinalQuery.Length,
            AssemblyTime: AssembledAt
        );
    }
}
