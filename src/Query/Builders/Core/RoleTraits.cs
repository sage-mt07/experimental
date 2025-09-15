using Kafka.Ksql.Linq.Query.Analysis;

namespace Kafka.Ksql.Linq.Query.Builders.Core;

/// <summary>
/// Describes window and emit behavior for each query role.
/// Final roles never compose intermediate sources; they operate on physical or view tables
/// and require windowing with <c>EMIT FINAL</c>.
/// </summary>
internal static class RoleTraits
{
    public static OperationSpec For(Role role)
    {
        return role switch
        {
            Role.Live => new(true, "CHANGES"),
            Role.Final => new(true, "FINAL"),
            Role.Final1s => new(true, "FINAL"),
            Role.Final1sStream => new(false, null),
            // Prev1m is a non-windowed table built via explicit join; no EMIT override
            Role.Prev1m => new(false, null),
            Role.Fill => new(true, "CHANGES"),
            _ => new(false, null)
        };
    }
}
