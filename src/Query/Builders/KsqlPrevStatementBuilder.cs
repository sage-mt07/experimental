using System;
using System.Linq;
using System.Text;

namespace Kafka.Ksql.Linq.Query.Builders;

/// <summary>
/// Builds CREATE TABLE AS SELECT for Prev role (e.g., prev_1m).
/// Produces a table aligned to the current bucket time whose values are sourced
/// from the previous bucket (offset by 1 minute by default).
/// </summary>
internal static class KsqlPrevStatementBuilder
{
    public static string Build(
        string name,
        string[] keys,
        string[] projection,
        string bucketColumn,
        string hbTable,
        string liveTable,
        int offsetMinutes = 1)
    {
        if (string.IsNullOrWhiteSpace(name)) throw new ArgumentException("name required", nameof(name));
        if (keys == null) keys = Array.Empty<string>();
        if (projection == null) projection = Array.Empty<string>();
        if (string.IsNullOrWhiteSpace(bucketColumn))
            throw new ArgumentException("bucketColumn required (must match WindowStart projection)", nameof(bucketColumn));
        if (string.IsNullOrWhiteSpace(hbTable)) throw new ArgumentException("hbTable required", nameof(hbTable));
        if (string.IsNullOrWhiteSpace(liveTable)) throw new ArgumentException("liveTable required", nameof(liveTable));

        var sb = new StringBuilder();
        sb.Append($"CREATE TABLE {name} WITH (KAFKA_TOPIC='{name}', KEY_FORMAT='AVRO', VALUE_FORMAT='AVRO') AS\n");

        var selectParts = new System.Collections.Generic.List<string>();
        foreach (var k in keys)
            selectParts.Add($"h.KEY->{k.ToUpperInvariant()} AS {k}");
        selectParts.Add($"h.{bucketColumn} AS {bucketColumn}");
        foreach (var col in projection)
        {
            if (keys.Contains(col, StringComparer.OrdinalIgnoreCase)) continue;
            if (string.Equals(col, bucketColumn, StringComparison.OrdinalIgnoreCase)) continue;
            selectParts.Add($"l.{col} AS {col}");
        }
        sb.Append("SELECT ").Append(string.Join(", ", selectParts)).AppendLine();

        // Join previous bucket by subtracting offset from current bucket time
        sb.Append($"FROM {hbTable} h LEFT JOIN {liveTable} l ON ");
        var onParts = new System.Collections.Generic.List<string>();
        foreach (var k in keys)
        {
            if (string.IsNullOrWhiteSpace(k)) continue;
            onParts.Add($"h.KEY->{k.ToUpperInvariant()} = l.KEY->{k.ToUpperInvariant()}");
        }
        onParts.Add($"l.{bucketColumn} = TIMESTAMPADD(MINUTES, -{offsetMinutes}, h.{bucketColumn})");
        sb.Append(string.Join(" AND ", onParts)).AppendLine();

        sb.Append("EMIT CHANGES;");
        return sb.ToString();
    }
}
