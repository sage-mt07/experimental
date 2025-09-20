using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Integration;

internal static class TestSchema
{
    public static readonly Dictionary<string, (string Name, string Type)[]> Tables = new()
    {
        ["orders"] = new[]
        {
            ("CustomerId", "INT"),
            ("Id", "INT"),
            ("Region", "VARCHAR"),
            ("Amount", "DOUBLE"),
            ("IsHighPriority", "BOOLEAN"),
            ("Count", "INT")
        },
        ["customers"] = new[]
        {
            ("Id", "INT"),
            ("Name", "VARCHAR")
        },
        ["events"] = new[]
        {
            ("Level", "INT"),
            ("Message", "VARCHAR")
        },
        ["orders_nullable"] = new[]
        {
            ("CustomerId", "INT"),
            ("Region", "VARCHAR"),
            ("Amount", "DOUBLE")
        },
        ["orders_nullable_key"] = new[]
        {
            ("CustomerId", "INT"),
            ("Amount", "DOUBLE")
        },
        // orders_multi_pk removed: multi-column PRIMARY KEY syntax is unsupported
    };

    // CompositePrimaryKeys dictionary removed per ksqlDB limitations

    public static IEnumerable<string> AllTableNames => Tables.Keys.Select(k => k.ToUpperInvariant());

    // Lowercase topic names for Schema Registry subjects
    public static IEnumerable<string> AllTopicNames => Tables.Keys;

    public static IEnumerable<string> AllColumns => Tables.Values.SelectMany(t => t.Select(c => c.Name.ToUpperInvariant())).Distinct();

    public static IEnumerable<string> GenerateTableDdls()
    {
        foreach (var kvp in Tables)
        {
            var cols = kvp.Value.Select((c, i) =>
                i == 0
                    ? $"{c.Name.ToUpperInvariant()} {c.Type} PRIMARY KEY"
                    : $"{c.Name.ToUpperInvariant()} {c.Type}");
            var colList = string.Join(", ", cols);
            yield return $"CREATE TABLE IF NOT EXISTS {kvp.Key.ToUpperInvariant()} ({colList}) WITH (KAFKA_TOPIC='{kvp.Key}', VALUE_FORMAT='AVRO', KEY_FORMAT='AVRO', PARTITIONS=1);";
        }
    }

    public static void ValidateDmlQuery(string query)
    {
        var tableRegex = new Regex(@"\b(FROM|JOIN|INTO|TABLE)\s+([A-Za-z_][A-Za-z0-9_]*)", RegexOptions.IgnoreCase);
        foreach (Match m in tableRegex.Matches(query))
        {
            var table = m.Groups[2].Value.ToUpperInvariant();
            Assert.Contains(table, AllTableNames.ToList());
        }

        var columnRegex = new Regex(@"\.([A-Za-z_][A-Za-z0-9_]*)");
        foreach (Match m in columnRegex.Matches(query))
        {
            var column = m.Groups[1].Value.ToUpperInvariant();
            Assert.Contains(column, AllColumns.ToList());
        }

        Assert.True(IsSupportedKsql(query), $"Unsupported function used: {query}");
    }

    private static readonly string[] UnsupportedFunctions = { "UPPER" };
    private static readonly Regex GroupByRegex = new(@"\bGROUP\s+BY\b", RegexOptions.IgnoreCase);
    private static readonly Regex AggregateRegex = new(@"\b(COUNT|SUM|AVG|MIN|MAX)\s*\(", RegexOptions.IgnoreCase);

    public static bool IsSupportedKsql(string query)
    {
        var upper = query.ToUpperInvariant();
        if (UnsupportedFunctions.Any(f => upper.Contains($"{f}(")))
            return false;

        bool isPullQuery = !upper.Contains("EMIT CHANGES");
        if (isPullQuery && (GroupByRegex.IsMatch(query) || AggregateRegex.IsMatch(query)))
            return false;

        return true;
    }
}
