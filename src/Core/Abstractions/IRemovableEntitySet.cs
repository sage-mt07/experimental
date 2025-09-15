using System.Threading;
using System.Threading.Tasks;
namespace Kafka.Ksql.Linq.Core.Abstractions;

/// <summary>
/// Optional interface for entity sets supporting removal of items.
/// </summary>
public interface IRemovableEntitySet<T> : IEntitySet<T> where T : class
{
    new Task RemoveAsync(T entity, CancellationToken cancellationToken = default);
}
