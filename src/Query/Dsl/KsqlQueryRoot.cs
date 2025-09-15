using System;

namespace Kafka.Ksql.Linq.Query.Dsl;

/// <summary>
/// Entry point for building a ToQuery DSL chain.
/// Provides the <c>From&lt;T&gt;()</c> method.
/// </summary>
public class KsqlQueryRoot
{
    private bool _fromCalled;

    public KsqlQueryable<T> From<T>()
    {
        if (_fromCalled)
            throw new InvalidOperationException("From() may only be called once in the current DSL chain.");

        _fromCalled = true;
        return new KsqlQueryable<T>();
    }
}
