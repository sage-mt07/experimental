using System.Threading.Tasks;

namespace Kafka.Ksql.Linq.Core.Abstractions;

public interface IErrorSink
{
    /// <summary>
    /// Process error records (e.g., send to DLQ)
    /// </summary>
    /// <param name="errorContext">Error context information</param>
    /// <param name="messageContext">Kafka message context</param>
    /// <returns>Task representing completion</returns>
    Task HandleErrorAsync(ErrorContext errorContext, KafkaMessageContext messageContext);

    /// <summary>
    /// Process error records (overload without message context)
    /// </summary>
    /// <param name="errorContext">Error context information</param>
    /// <returns>Task representing completion</returns>
    Task HandleErrorAsync(ErrorContext errorContext);

    /// <summary>
    /// Initialize the error sink
    /// </summary>
    /// <returns>Task representing initialization completion</returns>
    Task InitializeAsync();

    /// <summary>
    /// Clean up the error sink
    /// </summary>
    /// <returns>Task representing cleanup completion</returns>
    Task CleanupAsync();

    /// <summary>
    /// Whether the error sink is available
    /// </summary>
    bool IsAvailable { get; }
}
