using Kafka.Ksql.Linq.Core.Abstractions;
using System;

namespace Kafka.Ksql.Linq;

public static class EventSetErrorHandlingExtensions
{
    public static EventSet<T> OnError<T>(this EventSet<T> eventSet, ErrorAction errorAction) where T : class
    {
        if (typeof(T) == typeof(Messaging.DlqEnvelope) && errorAction == ErrorAction.DLQ)
            throw new InvalidOperationException("OnError(DLQ) cannot be used on a DLQ stream (to prevent infinite loops)");

        var policy = new ErrorHandlingPolicy
        {
            Action = errorAction
        };

        return eventSet.WithErrorPolicy(policy);
    }
}
