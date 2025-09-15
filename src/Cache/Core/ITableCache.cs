namespace Kafka.Ksql.Linq.Cache.Core;

internal interface ITableCache<T> : System.IDisposable where T : class
{
    System.Threading.Tasks.Task<System.Collections.Generic.List<T>> ToListAsync(
        System.Collections.Generic.List<string>? filter = null,
        System.TimeSpan? timeout = null);
}
