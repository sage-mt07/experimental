namespace Kafka.Ksql.Linq;

public record KsqlDbResponse(bool IsSuccess, string Message, int? ErrorCode = null, string? ErrorDetail = null);
