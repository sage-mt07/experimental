using System;

namespace Kafka.Ksql.Linq.Query.Pipeline;
internal record QueryAssemblyStats(
    int TotalParts,
    int RequiredParts,
    int OptionalParts,
    int QueryLength,
    DateTime AssemblyTime)
{
    /// <summary>
    /// Statistics summary
    /// </summary>
    public string GetSummary()
    {
        return $"Parts: {TotalParts} (Req:{RequiredParts}, Opt:{OptionalParts}), " +
               $"Length: {QueryLength}, " +
               $"Time: {AssemblyTime:HH:mm:ss.fff}";
    }
}
