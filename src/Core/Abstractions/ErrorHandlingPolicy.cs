using System;

namespace Kafka.Ksql.Linq.Core.Abstractions;

public class ErrorHandlingPolicy
{
    public ErrorAction Action { get; set; } = ErrorAction.Skip;
    public int RetryCount { get; set; } = 3;
    public TimeSpan RetryInterval { get; set; } = TimeSpan.FromSeconds(1);
    /// <summary>
    /// Custom error handler
    /// </summary>
    public Func<ErrorContext, object, bool>? CustomHandler { get; set; }

    /// <summary>
    /// Retry condition check
    /// </summary>
    public Predicate<Exception>? RetryCondition { get; set; }


    /// <summary>
    /// Additional log information on error
    /// </summary>
    public Func<ErrorContext, string>? AdditionalLogInfo { get; set; }

    /// <summary>
    /// Dynamic calculation of retry interval
    /// </summary>
    public Func<int, TimeSpan>? DynamicRetryInterval { get; set; }
    public static ErrorHandlingPolicy ExponentialBackoff(int maxRetries = 3, TimeSpan baseInterval = default)
    {
        var interval = baseInterval == default ? TimeSpan.FromSeconds(1) : baseInterval;
        return new ErrorHandlingPolicy
        {
            Action = ErrorAction.Retry,
            RetryCount = maxRetries,
            DynamicRetryInterval = attempt => TimeSpan.FromMilliseconds(
                interval.TotalMilliseconds * Math.Pow(2, attempt - 1))
        };
    }

    public static ErrorHandlingPolicy CircuitBreaker(int failureThreshold = 5, TimeSpan recoveryInterval = default)
    {
        var recovery = recoveryInterval == default ? TimeSpan.FromMinutes(1) : recoveryInterval;
        return new ErrorHandlingPolicy
        {
            Action = ErrorAction.Skip,
            CustomHandler = new CircuitBreakerHandler(failureThreshold, recovery).Handle
        };
    }

}
