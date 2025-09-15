using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Messaging;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Kafka.Ksql.Linq.Tests;

#nullable enable

internal class StubEntitySet<T> : IEntitySet<T> where T : class
{
    public List<(T Entity, Dictionary<string, string>? Headers)> Added { get; } = new();
    public Task AddAsync(T entity, Dictionary<string, string>? headers = null, CancellationToken cancellationToken = default)
    {
        Added.Add((entity, headers));
        return Task.CompletedTask;
    }
    public Task RemoveAsync(T entity, CancellationToken cancellationToken = default) => Task.CompletedTask;
    public Task<List<T>> ToListAsync(CancellationToken cancellationToken = default) => Task.FromResult(new List<T>());
    public Task ForEachAsync(Func<T, Task> action, TimeSpan timeout = default, bool autoCommit = true, CancellationToken cancellationToken = default) => Task.CompletedTask;

    [Obsolete("Use ForEachAsync(Func<T, Dictionary<string,string>, MessageMeta, Task>)")]
    public Task ForEachAsync(Func<T, Dictionary<string, string>, Task> action, TimeSpan timeout = default, bool autoCommit = true, CancellationToken cancellationToken = default) => Task.CompletedTask;

    public Task ForEachAsync(Func<T, Dictionary<string, string>, MessageMeta, Task> action, TimeSpan timeout = default, bool autoCommit = true, CancellationToken cancellationToken = default) => Task.CompletedTask;
    public string GetTopicName() => "stub";
    public EntityModel GetEntityModel() => new EntityModel { EntityType = typeof(T) };
    public IKsqlContext GetContext() => throw new NotImplementedException();
    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default) => throw new NotImplementedException();
}
