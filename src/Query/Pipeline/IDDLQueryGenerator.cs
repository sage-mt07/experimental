using Kafka.Ksql.Linq.Query.Ddl;
using System.Linq.Expressions;

namespace Kafka.Ksql.Linq.Query.Pipeline;

internal interface IDDLQueryGenerator
{
    string GenerateCreateStream(IDdlSchemaProvider provider);
    string GenerateCreateTable(IDdlSchemaProvider provider);
    string GenerateCreateStreamAs(string streamName, string baseObject, Expression linqExpression);
    string GenerateCreateTableAs(string tableName, string baseObject, Expression linqExpression);
}