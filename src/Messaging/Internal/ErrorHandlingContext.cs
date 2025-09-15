using Kafka.Ksql.Linq.Core.Abstractions;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace Kafka.Ksql.Linq.Messaging.Internal;
public class ErrorHandlingContext
{
    /// <summary>
    /// Action to take when an error occurs
    /// </summary>
    public ErrorAction ErrorAction { get; set; } = ErrorAction.Skip;

    private static readonly ILogger Logger = LoggerFactory.Create(builder => builder.AddConsole()).CreateLogger<ErrorHandlingContext>();

    /// <summary>
    /// Number of retry attempts
    /// </summary>
    public int RetryCount { get; set; } = 3;

    /// <summary>
    /// Interval between retries
    /// </summary>
    public TimeSpan RetryInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Current attempt count (internal use)
    /// </summary>
    public int CurrentAttempt { get; set; } = 0;

    /// <summary>
    /// Event triggered when an error occurs
    /// </summary>
    public event Func<ErrorContext, KafkaMessageContext, Task>? ErrorOccurred;

    /// <summary>
    /// Custom error handler (type-safe)
    /// </summary>
    public Func<ErrorContext, object, bool>? CustomHandler { get; set; }

    /// <summary>
    /// Execute error handling
    /// </summary>
    /// <param name="originalItem">Original item</param>
    /// <param name="exception">Raised exception</param>
    /// <param name="messageContext">Message context</param>
    /// <returns>Whether processing continues (false = skip, true = continue/rethrow)</returns>
    public async Task<bool> HandleErrorAsync<T>(T originalItem, Exception exception, KafkaMessageContext messageContext)
    {
        // Execute custom handler first if configured
        if (ErrorAction == ErrorAction.Skip && CustomHandler != null)
        {
            var errorContext = new ErrorContext
            {
                Exception = exception,
                OriginalMessage = originalItem,
                AttemptCount = CurrentAttempt,
                FirstAttemptTime = DateTime.UtcNow.AddSeconds(-CurrentAttempt * RetryInterval.TotalSeconds),
                LastAttemptTime = DateTime.UtcNow,
                ErrorPhase = "Processing"
            };

            try
            {
                return CustomHandler(errorContext, originalItem!);
            }
            catch (Exception handlerEx)
            {
                Logger.LogError(handlerEx, "Failed in custom handler");
                return false; // Skip if custom handler throws
            }
        }

        switch (ErrorAction)
        {
            case ErrorAction.Skip:
                // Log the error and skip
                Logger.LogWarning(exception, "Skipping item after error");
                return false; // Skip

            case ErrorAction.Retry:
                // Retry logic handled by ProcessItemWithErrorHandling
                // Notify error event after final attempt
                if (ErrorOccurred != null)
                {
                    var errorContext = new ErrorContext
                    {
                        Exception = exception,
                        OriginalMessage = originalItem,
                        AttemptCount = CurrentAttempt,
                        FirstAttemptTime = DateTime.UtcNow.AddSeconds(-CurrentAttempt * RetryInterval.TotalSeconds),
                        LastAttemptTime = DateTime.UtcNow,
                        ErrorPhase = "Processing"
                    };
                    await ErrorOccurred.Invoke(errorContext, messageContext);
                }
                return false; // Skip

            case ErrorAction.DLQ:
                // Notify error event
                if (ErrorOccurred != null)
                {
                    var errorContext = new ErrorContext
                    {
                        Exception = exception,
                        OriginalMessage = originalItem,
                        AttemptCount = CurrentAttempt,
                        FirstAttemptTime = DateTime.UtcNow.AddSeconds(-CurrentAttempt * RetryInterval.TotalSeconds),
                        LastAttemptTime = DateTime.UtcNow,
                        ErrorPhase = "Processing"
                    };
                    await ErrorOccurred.Invoke(errorContext, messageContext);
                }
                return false; // Skip

            default:
                // Skip if action is unknown
                Logger.LogError("Skipping item due to unknown error action: {Action}", ErrorAction);
                return false;
        }
    }

}
