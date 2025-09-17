namespace Kafka.Ksql.Linq.Configuration;

public class KsqlServerOptions
{
    /// <summary>
    /// Value of ksql.service.id used by the ksqlDB server.
    /// </summary>
    public string ServiceId { get; init; } = "ksql_service_1";

    /// <summary>
    /// Value of ksql.streams.persistent.query.name.prefix used by the ksqlDB server.
    /// </summary>
    public string PersistentQueryPrefix { get; init; } = "query_";
}
