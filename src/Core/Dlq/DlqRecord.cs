using System.Collections.Generic;

namespace Kafka.Ksql.Linq.Core.Dlq;

public sealed record DlqRecord(
    string MessageId,
    string Topic,
    int Partition,
    long Offset,
    string TimestampUtc,
    byte[] RawBytes,
    string? RawText,
    string PayloadFormat,
    string? SchemaId,
    string? SchemaSubject,
    int? SchemaVersion,
    IReadOnlyDictionary<string, string> Headers,
    string? ErrorType,
    string? ErrorMessage,
    string? StackTrace
);
