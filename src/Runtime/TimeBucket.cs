using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Ksql.Linq.Cache.Extensions;

namespace Kafka.Ksql.Linq.Runtime;

/// <summary>
/// Write-context for importing bar data into time-bucketed topics.
/// An application (importer) should implement this to map to its producer.
/// </summary>
// Interfaces removed: unified on KsqlContext for read/write

public static class TimeBucket
{
    public static TimeBucket<T> Get<T>(Kafka.Ksql.Linq.KsqlContext ctx, Period period) where T : class
        => new(ctx, period);

    public static TimeBucketWriter<T> Set<T>(Kafka.Ksql.Linq.KsqlContext ctx, Period period) where T : class
        => new(ctx, period);
}

public sealed class TimeBucket<T> where T : class
{
    private readonly Kafka.Ksql.Linq.KsqlContext _ctx;
    private readonly Period _period;
    private readonly string? _finalTopic;
    private readonly string? _liveTopic;
    private readonly Type _readType;
    private readonly Type _writeType;

    internal TimeBucket(Kafka.Ksql.Linq.KsqlContext ctx, Period period)
    {
        _ctx = ctx ?? throw new ArgumentNullException(nameof(ctx));
        _period = period;
        var baseTopic = _ctx.GetTopicName<T>();
        var prefix = $"{baseTopic}_{period}";
        if (period.Unit == PeriodUnit.Seconds && period.Value == 1)
        {
            _finalTopic = $"{prefix}_final";
            _liveTopic = null;
        }
        else
        {
            _finalTopic = null;
            _liveTopic = $"{prefix}_live";
        }
        _readType = TimeBucketTypes.ResolveRead(typeof(T), period) ?? typeof(T);
        _writeType = TimeBucketTypes.ResolveWrite(typeof(T), period) ?? typeof(T);
    }

    public async Task<List<T>> ToListAsync(IReadOnlyList<string> pkFilter, CancellationToken ct)
    {
        if (pkFilter == null) throw new ArgumentNullException(nameof(pkFilter));
        if (_period.Unit == PeriodUnit.Seconds)
            throw new ArgumentOutOfRangeException(nameof(_period), "Period must be minutes or greater.");

        // Resolve TableCache for the read type and pass pkFilter as prefix parts
        var getCache = typeof(Kafka.Ksql.Linq.Cache.Extensions.KsqlContextCacheExtensions)
            .GetMethod("GetTableCache", System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic)!;
        var getCacheGeneric = getCache.MakeGenericMethod(_readType);
        var cache = getCacheGeneric.Invoke(null, new object?[] { _ctx });
        if (cache == null)
            throw new InvalidOperationException("Table cache not available for TimeBucket read.");

        var filter = new List<string>(pkFilter);
        var toList = cache.GetType().GetMethod("ToListAsync", new[] { typeof(List<string>), typeof(TimeSpan?) });
        var taskObj = toList!.Invoke(cache, new object?[] { filter, (TimeSpan?)null })!;
        var task = (System.Threading.Tasks.Task)taskObj;
        await task.ConfigureAwait(false);
        var resultProp = taskObj.GetType().GetProperty("Result")!;
        var resultEnum = (IEnumerable)resultProp.GetValue(taskObj)!;
        var list = new List<T>();
        foreach (var item in resultEnum) list.Add((T)item);
        if (list.Count == 0)
            throw new InvalidOperationException("No rows matched the filter.");
        return list;
    }

    internal string? FinalTopicName => _finalTopic;
    internal string? LiveTopicName => _liveTopic;
}

/// <summary>
/// Writer counterpart to <see cref="TimeBucket{T}"/> for importing bars.
/// </summary>
public sealed class TimeBucketWriter<T> where T : class
{
    private readonly Kafka.Ksql.Linq.KsqlContext _ctx;
    private readonly string? _finalTopic;
    private readonly string? _liveTopic;

    internal TimeBucketWriter(Kafka.Ksql.Linq.KsqlContext ctx, Period period)
    {
        _ctx = ctx ?? throw new ArgumentNullException(nameof(ctx));
        var baseTopic = typeof(T).Name.ToLowerInvariant();
        var prefix = $"{baseTopic}_{period}";
        if (period.Unit == PeriodUnit.Seconds && period.Value == 1)
        {
            _finalTopic = $"{prefix}_final";
            _liveTopic = null;
        }
        else
        {
            _finalTopic = null;
            _liveTopic = $"{prefix}_live";
        }
    }

    public Task WriteAsync(T row, CancellationToken ct = default)
        => _ctx.Set<T>().AddAsync(row, null, ct);

    internal string? FinalTopicName => _finalTopic;
    internal string? LiveTopicName => _liveTopic;
}
