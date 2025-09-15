namespace Kafka.Ksql.Linq.Query.Builders.Schema;

public record KsqlColumn(string Name, string KsqlType, bool IsKey = false);
