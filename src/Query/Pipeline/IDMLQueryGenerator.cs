using System.Linq.Expressions;

namespace Kafka.Ksql.Linq.Query.Pipeline;

public interface IDMLQueryGenerator
{
    string GenerateSelectAll(string objectName, bool isPullQuery = true, bool isTableQuery = false);
    string GenerateSelectWithCondition(string objectName, Expression whereExpression, bool isPullQuery = true, bool isTableQuery = false);
    string GenerateCountQuery(string objectName);
    string GenerateAggregateQuery(string objectName, Expression aggregateExpression);
    string GenerateLinqQuery(string objectName, Expression linqExpression, bool isPullQuery = false, bool isTableQuery = false);
}
