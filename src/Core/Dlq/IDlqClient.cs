using System.Collections.Generic;
using System.Threading;

namespace Kafka.Ksql.Linq.Core.Dlq;

public interface IDlqClient
{
    /// <summary>
    /// Read DLQ records sequentially and attach human-readable RawText.
    /// On errors, still return a single record using a fallback (e.g., head of Base64).
    /// </summary>
    IAsyncEnumerable<DlqRecord> ReadAsync(
        DlqReadOptions? options = null,
        CancellationToken ct = default);
}
