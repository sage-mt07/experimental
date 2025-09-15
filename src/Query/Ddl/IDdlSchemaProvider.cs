namespace Kafka.Ksql.Linq.Query.Ddl;

public interface IDdlSchemaProvider
{
    DdlSchemaDefinition GetSchema();
}
