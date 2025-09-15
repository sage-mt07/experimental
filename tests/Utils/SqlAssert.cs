using System.Text.RegularExpressions;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Utils;

public static class SqlAssert
{
    public static void EqualNormalized(string expected, string actual)
        => Assert.Equal(Normalize(expected), Normalize(actual));

    public static void ContainsNormalized(string actual, string expectedFragment)
        => Assert.Contains(Normalize(expectedFragment), Normalize(actual));

    public static void StartsWithNormalized(string actual, string expectedPrefix)
        => Assert.StartsWith(Normalize(expectedPrefix), Normalize(actual));

    public static void EndsWithSemicolon(string actual)
        => Assert.EndsWith(";", actual.Trim());

    public static string Normalize(string s)
    {
        if (s == null) return string.Empty;
        // Normalize newlines and collapse whitespace
        var n = s.Replace("\r\n", "\n").Replace("\r", "\n");
        // Remove excessive spaces around punctuation
        n = Regex.Replace(n, @"\s+", " ");
        n = Regex.Replace(n, @"\s*\(\s*", "(");
        n = Regex.Replace(n, @"\s*\)\s*", ")");
        n = Regex.Replace(n, @"\s*,\s*", ", ");
        n = Regex.Replace(n, @"\s*;\s*", ";");
        n = n.Trim();
        // Case-insensitive comparison by lowering both sides
        return n.ToLowerInvariant();
    }

    public static void AssertOrderNormalized(string actual, params string[] fragments)
    {
        var norm = Normalize(actual);
        var prev = -1;
        foreach (var f in fragments)
        {
            var frag = Normalize(f);
            var idx = norm.IndexOf(frag, System.StringComparison.Ordinal);
            Assert.True(idx >= 0, $"Fragment not found: '{f}'\nIn:\n{actual}");
            Assert.True(idx > prev, $"Order mismatch: '{f}' appears out of sequence.\nIn:\n{actual}");
            prev = idx;
        }
    }
}
