namespace Kafka.Ksql.Linq.Infrastructure.KsqlDb;

using System; // for TimeSpan
using System.Collections.Generic;
using System.Threading.Tasks;

public interface IKsqlDbClient
{
    Task<Kafka.Ksql.Linq.KsqlDbResponse> ExecuteStatementAsync(string statement);
    Task<Kafka.Ksql.Linq.KsqlDbResponse> ExecuteExplainAsync(string ksql);
    /// <summary>
    /// Executes SHOW TABLES and returns the Kafka topic names of all tables.
    /// </summary>
    Task<HashSet<string>> GetTableTopicsAsync();

    /// <summary>
    /// Execute a query via /query-stream and count the number of rows returned.
    /// Intended for assertions with SELECT ... EMIT CHANGES LIMIT N.
    /// </summary>
    Task<int> ExecuteQueryStreamCountAsync(string sql, TimeSpan? timeout = null);

    /// <summary>
    /// Execute a pull query via /query and return row count.
    /// Suitable for SELECT ... FROM TABLE [WHERE ...] LIMIT N (no EMIT CHANGES).
    /// </summary>
    Task<int> ExecutePullQueryCountAsync(string sql, TimeSpan? timeout = null);
}
