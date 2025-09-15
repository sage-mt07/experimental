using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Messaging;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace Kafka.Ksql.Linq.Query;

internal class TypedJoinResultEntitySet<TOuter, TInner, TResult> : IEntitySet<TResult>
       where TOuter : class
       where TInner : class
       where TResult : class
{
    private readonly IKsqlContext _context;
    private readonly EntityModel _entityModel;
    private readonly IEntitySet<TOuter> _outerEntitySet;
    private readonly IEntitySet<TInner> _innerEntitySet;
    private readonly Expression<Func<TOuter, object>> _outerKeySelector;
    private readonly Expression<Func<TInner, object>> _innerKeySelector;
    private readonly Expression<Func<TOuter, TInner, TResult>> _resultSelector;

    public TypedJoinResultEntitySet(
        IKsqlContext context,
        EntityModel entityModel,
        IEntitySet<TOuter> outerEntitySet,
        IEntitySet<TInner> innerEntitySet,
        Expression<Func<TOuter, object>> outerKeySelector,
        Expression<Func<TInner, object>> innerKeySelector,
        Expression<Func<TOuter, TInner, TResult>> resultSelector)
    {
        _context = context ?? throw new ArgumentNullException(nameof(context));
        _entityModel = entityModel ?? throw new ArgumentNullException(nameof(entityModel));
        _outerEntitySet = outerEntitySet ?? throw new ArgumentNullException(nameof(outerEntitySet));
        _innerEntitySet = innerEntitySet ?? throw new ArgumentNullException(nameof(innerEntitySet));
        _outerKeySelector = outerKeySelector ?? throw new ArgumentNullException(nameof(outerKeySelector));
        _innerKeySelector = innerKeySelector ?? throw new ArgumentNullException(nameof(innerKeySelector));
        _resultSelector = resultSelector ?? throw new ArgumentNullException(nameof(resultSelector));
    }

    public async Task<List<TResult>> ToListAsync(CancellationToken cancellationToken = default)
    {
        // Simplified JOIN processing placeholder
        await Task.Delay(100, cancellationToken);
        return new List<TResult>();
    }

    public Task AddAsync(TResult entity, Dictionary<string, string>? headers = null, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException("Cannot add entities to a join result set");
    }

    public Task RemoveAsync(TResult entity, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException("Cannot remove entities from a join result set");
    }

    public Task ForEachAsync(Func<TResult, Task> action, TimeSpan timeout = default, bool autoCommit = true, CancellationToken cancellationToken = default)
        => throw new NotSupportedException("ForEachAsync not supported on join result sets");

    [Obsolete("Use ForEachAsync(Func<TResult, Dictionary<string,string>, MessageMeta, Task>)")]
    public Task ForEachAsync(Func<TResult, Dictionary<string, string>, Task> action, TimeSpan timeout = default, bool autoCommit = true, CancellationToken cancellationToken = default)
        => throw new NotSupportedException("ForEachAsync not supported on join result sets");

    public Task ForEachAsync(Func<TResult, Dictionary<string, string>, MessageMeta, Task> action, TimeSpan timeout = default, bool autoCommit = true, CancellationToken cancellationToken = default)
        => throw new NotSupportedException("ForEachAsync not supported on join result sets");

    public string GetTopicName() => (_entityModel.TopicName ?? typeof(TResult).Name).ToLowerInvariant();
    public EntityModel GetEntityModel() => _entityModel;
    public IKsqlContext GetContext() => _context;

    public async IAsyncEnumerator<TResult> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        var results = await ToListAsync(cancellationToken);
        foreach (var item in results)
        {
            yield return item;
        }
    }
}
