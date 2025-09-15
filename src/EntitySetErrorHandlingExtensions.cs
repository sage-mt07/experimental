using Kafka.Ksql.Linq.Core.Abstractions;
using System;

namespace Kafka.Ksql.Linq;

/// <summary>
/// Extensions to enable error handling on IEntitySet when the underlying
/// implementation derives from EventSet.
/// </summary>
public static class EntitySetErrorHandlingExtensions
{
    /// <summary>
    /// Applies error handling policy to the entity set.
    /// </summary>
    public static EventSet<T> OnError<T>(this IEntitySet<T> entitySet, ErrorAction errorAction) where T : class
    {
        if (entitySet is EventSet<T> eventSet)
        {
            return eventSet.OnError(errorAction);
        }
        throw new InvalidOperationException("OnError is only supported on EventSet-based entity sets.");
    }
}

