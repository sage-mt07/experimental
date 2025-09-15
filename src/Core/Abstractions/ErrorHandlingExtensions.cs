using System;

namespace Kafka.Ksql.Linq.Core.Abstractions;

public static class ErrorHandlingExtensions
{
    /// <summary>
    /// Configure a custom error handler
    /// </summary>
    public static EventSet<T> OnError<T>(this EventSet<T> eventSet,
        Func<ErrorContext, T, bool> customHandler) where T : class
    {
        if (customHandler == null)
            throw new ArgumentNullException(nameof(customHandler));

        Func<ErrorContext, object, bool> wrappedHandler = (errorContext, originalMessage) =>
        {
            if (originalMessage is T typedMessage)
            {
                return customHandler(errorContext, typedMessage);
            }
            return false; // Skip when the type doesn't match
        };
        var policy = new ErrorHandlingPolicy
        {
            Action = ErrorAction.Skip, // Skip when using a custom handler
            CustomHandler = wrappedHandler
        };

        return eventSet.WithErrorPolicy(policy);
    }

    /// <summary>
    /// Configure conditional retry
    /// </summary>
    public static EventSet<T> WithRetryWhen<T>(this EventSet<T> eventSet,
        Predicate<Exception> retryCondition,
        int retryCount = 3,
        TimeSpan? retryInterval = null) where T : class
    {
        if (retryCondition == null)
            throw new ArgumentNullException(nameof(retryCondition));

        var policy = new ErrorHandlingPolicy
        {
            Action = ErrorAction.Retry,
            RetryCount = retryCount,
            RetryInterval = retryInterval ?? TimeSpan.FromSeconds(1),
            RetryCondition = retryCondition
        };

        return eventSet.WithErrorPolicy(policy);
    }

}
