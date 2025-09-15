using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text.RegularExpressions;

namespace Kafka.Ksql.Linq.Messaging;

public static class DlqEnvelopeFactory
{
    private static int? TryGetSchemaId(byte[]? payload)
        => (payload is { Length: >= 5 } && payload[0] == 0)
            ? System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(payload.AsSpan(1, 4))
            : null;

    private static string TrimStr(string? s, int max)
    {
        if (string.IsNullOrEmpty(s)) return string.Empty;
        return s.Length <= max ? s : s.Substring(0, max);
    }

    private static string Fingerprint(string msg, string? stack)
    {
        var text = (msg ?? string.Empty) + "\n" + (stack ?? string.Empty);
        using var sha = System.Security.Cryptography.SHA256.Create();
        return Convert.ToHexString(sha.ComputeHash(System.Text.Encoding.UTF8.GetBytes(text)));
    }

    public static DlqEnvelope From<TKey, TValue>(
        ConsumeResult<TKey, TValue> r,
        Exception ex,
        string? applicationId,
        string? consumerGroup,
        string? host,
        IReadOnlyDictionary<string, string> headerAllowList,
        int maxMsg = 1024,
        int maxStack = 2048,
        bool normalizeStack = false)
        where TKey : class
        where TValue : class
    {
        var msgShort = TrimStr(ex.Message, maxMsg);
        var stack = ex.StackTrace;
        if (normalizeStack && stack is not null)
            stack = Regex.Replace(stack, @"\s+", " ").Trim();
        var stackShort = stack is { Length: > 0 } st ? TrimStr(st, maxStack) : null;

        return new DlqEnvelope
        {
            Topic = r.Topic,
            Partition = r.Partition,
            Offset = r.Offset,
            TimestampUtc = r.Message.Timestamp.UtcDateTime.ToString("o"),
            IngestedAtUtc = DateTimeOffset.UtcNow.ToString("o"),

            PayloadFormatKey = (r.Message.Key as byte[]) is not null ? "avro" : "none",
            PayloadFormatValue = (r.Message.Value as byte[]) is not null ? "avro" : "none",
            SchemaIdKey = ToStrOrEmpty(TryGetSchemaId(r.Message.Key as byte[])),
            SchemaIdValue = ToStrOrEmpty(TryGetSchemaId(r.Message.Value as byte[])),
            KeyIsNull = r.Message.Key is null,

            ErrorType = ex.GetType().Name,
            ErrorMessageShort = msgShort,
            StackTraceShort = stackShort,
            ErrorFingerprint = Fingerprint(msgShort, stackShort),

            ApplicationId = applicationId,
            ConsumerGroup = consumerGroup,
            Host = host,

            Headers = new Dictionary<string, string>(headerAllowList, StringComparer.OrdinalIgnoreCase)
        };
    }
    private static string ToStrOrEmpty(int? v) => v?.ToString(CultureInfo.InvariantCulture) ?? string.Empty;

    private static string? ToStr(int? v) => v?.ToString(CultureInfo.InvariantCulture);
    public static DlqEnvelope From(
        MessageMeta meta,
        Exception ex,
        string? applicationId,
        string? consumerGroup,
        string? host,
        int maxMsg = 1024,
        int maxStack = 2048,
        bool normalizeStack = false)
    {
        var msgShort = TrimStr(ex.Message, maxMsg);
        var stack = ex.StackTrace;
        if (normalizeStack && stack is not null)
            stack = Regex.Replace(stack, @"\s+", " ").Trim();
        var stackShort = stack is { Length: > 0 } st ? TrimStr(st, maxStack) : null;

        return new DlqEnvelope
        {
            Topic = meta.Topic,
            Partition = meta.Partition,
            Offset = meta.Offset,
            TimestampUtc = meta.TimestampUtc.ToString("o"),
            IngestedAtUtc = DateTimeOffset.UtcNow.ToString("o"),

            PayloadFormatKey = meta.SchemaIdKey is null ? "none" : "avro",
            PayloadFormatValue = meta.SchemaIdValue is null ? "none" : "avro",
            SchemaIdKey = ToStrOrEmpty(meta.SchemaIdKey),
            SchemaIdValue = ToStrOrEmpty(meta.SchemaIdValue),
            KeyIsNull = meta.KeyIsNull,

            ErrorType = ex.GetType().Name,
            ErrorMessageShort = msgShort,
            StackTraceShort = stackShort,
            ErrorFingerprint = Fingerprint(msgShort, stackShort),

            ApplicationId = applicationId,
            ConsumerGroup = consumerGroup,
            Host = host,

            Headers = new Dictionary<string, string>(meta.HeaderAllowList, StringComparer.OrdinalIgnoreCase)
        };
    }
}
