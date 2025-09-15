namespace Kafka.Ksql.Linq.Query.Ddl;

public record ColumnDefinition(string Name, string Type, bool IsKey);
