using Kafka.Ksql.Linq.Cache.Extensions;
using Kafka.Ksql.Linq.Core.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Kafka.Ksql.Linq.Cache.Core;

internal class ReadCachedEntitySet<T> : EventSet<T> where T : class
{
    private readonly ILogger<ReadCachedEntitySet<T>> _logger;
    private readonly EventSet<T> _baseSet;

    internal ReadCachedEntitySet(IKsqlContext context, EntityModel model, ILoggerFactory? loggerFactory = null, EventSet<T>? baseSet = null)
        : base(context, model, commitManager: (context as KsqlContext)?.GetCommitManager())
    {
        _logger = loggerFactory?.CreateLogger<ReadCachedEntitySet<T>>() ?? NullLogger<ReadCachedEntitySet<T>>.Instance;
        _baseSet = baseSet ?? throw new InvalidOperationException("Writable base set is not available for AddAsync.");
    }

    public override async Task<List<T>> ToListAsync(CancellationToken cancellationToken = default)
    {
        var cache = _context.GetTableCache<T>();
        if (cache == null)
        {
            _logger.LogWarning("Table cache not available for {Entity}", typeof(T).Name);
            return new List<T>();
        }

        return await cache.ToListAsync();
    }

    public override async IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        foreach (var item in await ToListAsync(cancellationToken))
            yield return item;
    }

    protected override Task SendEntityAsync(T entity, Dictionary<string, string>? headers, CancellationToken cancellationToken)
    {
        return _baseSet.AddAsync(entity, headers, cancellationToken);
    }
}

