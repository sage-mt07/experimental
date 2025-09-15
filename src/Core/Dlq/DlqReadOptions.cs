namespace Kafka.Ksql.Linq.Core.Dlq;

public sealed class DlqReadOptions
{
    public bool FromBeginning { get; init; } = false;
    public int MaxBytesForRawText { get; init; } = 64 * 1024;
    public bool CommitOnRead { get; init; } = true;
}
