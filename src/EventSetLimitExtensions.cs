using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Query.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Kafka.Ksql.Linq;


/// <summary>
/// Extensions for limiting the number of items in an entity set.
/// </summary>
public static class EventSetLimitExtensions
{
    /// <summary>
    /// Returns the newest <paramref name="count"/> items ordered by BarTime and removes older items when supported.
    /// </summary>
    public static async Task<List<T>> Limit<T>(this IEntitySet<T> entitySet, int count, CancellationToken cancellationToken = default) where T : class
    {
        if (entitySet == null) throw new ArgumentNullException(nameof(entitySet));
        if (count < 0) throw new ArgumentOutOfRangeException(nameof(count));

        var items = await entitySet.ToListAsync(cancellationToken);
        var model = entitySet.GetEntityModel();
        if (model.GetExplicitStreamTableType() != StreamTableType.Table)
            throw new NotSupportedException("Limit is only supported for Table entities.");
        if (model.BarTimeSelector == null)
            throw new InvalidOperationException($"Entity {typeof(T).Name} is missing bar time selector configuration.");

        var selector = (Func<T, DateTime>)model.BarTimeSelector.Compile();
        var ordered = items.OrderByDescending(selector).ToList();
        var toKeep = ordered.Take(count).ToList();
        var toRemove = ordered.Skip(count).ToList();

        if (entitySet is IRemovableEntitySet<T> removable)
        {
            foreach (var item in toRemove)
            {
                await removable.RemoveAsync(item, cancellationToken);
            }
        }

        return toKeep;

    }
}
