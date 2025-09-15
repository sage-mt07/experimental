using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Messaging;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace Kafka.Ksql.Linq.Query;

/// <summary>
/// EntitySet wrapper that supports JOIN operations.
/// </summary>
public class JoinableEntitySet<T> : IEntitySet<T>, IJoinableEntitySet<T> where T : class
{
    private readonly IEntitySet<T> _baseEntitySet;

    public JoinableEntitySet(IEntitySet<T> baseEntitySet)
    {
        _baseEntitySet = baseEntitySet ?? throw new ArgumentNullException(nameof(baseEntitySet));
    }

    public Task AddAsync(T entity, Dictionary<string, string>? headers = null, CancellationToken cancellationToken = default)
    {
        return _baseEntitySet.AddAsync(entity, headers, cancellationToken);
    }

    public Task RemoveAsync(T entity, CancellationToken cancellationToken = default)
    {
        return _baseEntitySet.RemoveAsync(entity, cancellationToken);
    }

    public Task<List<T>> ToListAsync(CancellationToken cancellationToken = default)
    {
        return _baseEntitySet.ToListAsync(cancellationToken);
    }

    public Task ForEachAsync(Func<T, Task> action, TimeSpan timeout = default, bool autoCommit = true, CancellationToken cancellationToken = default)
    {
        return _baseEntitySet.ForEachAsync((e, h, _) => action(e), timeout, autoCommit, cancellationToken);
    }

    [Obsolete("Use ForEachAsync(Func<T, Dictionary<string,string>, MessageMeta, Task>)")]
    public Task ForEachAsync(Func<T, Dictionary<string, string>, Task> action, TimeSpan timeout = default, bool autoCommit = true, CancellationToken cancellationToken = default)
        => _baseEntitySet.ForEachAsync((e, h, _) => action(e, h), timeout, autoCommit, cancellationToken);

    public Task ForEachAsync(Func<T, Dictionary<string, string>, MessageMeta, Task> action, TimeSpan timeout = default, bool autoCommit = true, CancellationToken cancellationToken = default)
    {
        return _baseEntitySet.ForEachAsync(action, timeout, autoCommit, cancellationToken);
    }

    public string GetTopicName() => _baseEntitySet.GetTopicName();

    public EntityModel GetEntityModel() => _baseEntitySet.GetEntityModel();

    public IKsqlContext GetContext() => _baseEntitySet.GetContext();

    public async IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        await foreach (var item in _baseEntitySet.WithCancellation(cancellationToken))
        {
            yield return item;
        }
    }

    public IJoinResult<T, TInner> Join<TInner, TKey>(
        IEntitySet<TInner> inner,
        Expression<Func<T, TKey>> outerKeySelector,
        Expression<Func<TInner, TKey>> innerKeySelector) where TInner : class
    {
        if (inner == null)
            throw new ArgumentNullException(nameof(inner));
        if (outerKeySelector == null)
            throw new ArgumentNullException(nameof(outerKeySelector));
        if (innerKeySelector == null)
            throw new ArgumentNullException(nameof(innerKeySelector));

        return new JoinResult<T, TInner>(this, inner, outerKeySelector, innerKeySelector);
    }

    public override string ToString()
    {
        return $"JoinableEntitySet<{typeof(T).Name}> wrapping {_baseEntitySet}";
    }
}
