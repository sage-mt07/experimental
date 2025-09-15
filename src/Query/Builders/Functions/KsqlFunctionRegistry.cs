using Kafka.Ksql.Linq.Configuration;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Ksql.Linq.Query.Builders.Functions;

/// <summary>
/// KSQL function registry
/// Design rationale: comprehensive mapping from C# method names to KSQL functions
/// </summary>
internal static class KsqlFunctionRegistry
{
    private static readonly Dictionary<string, KsqlFunctionMapping> _functionMappings = new()
    {
        // String functions (fully supported)
        ["ToUpper"] = new("UPPER", 1),
        ["ToLower"] = new("LOWER", 1),
        ["Substring"] = new("SUBSTRING", 2, 3),
        ["Length"] = new("LEN", 1),
        ["Trim"] = new("TRIM", 1),
        ["Replace"] = new("REPLACE", 3),
        ["Contains"] = new("INSTR({0}, {1}) > 0", 2, "INSTR({0}, {1}) > 0"),
        ["StartsWith"] = new("STARTS_WITH", 2),
        ["EndsWith"] = new("ENDS_WITH", 2),
        ["Split"] = new("SPLIT", 2),
        ["Concat"] = new("CONCAT", 2, int.MaxValue),
        ["IndexOf"] = new("INSTR", 2),
        ["PadLeft"] = new("LPAD", 2, 3),
        ["PadRight"] = new("RPAD", 2, 3),

        // Numeric functions (fully supported)
        ["Abs"] = new("ABS", 1),
        ["Round"] = new("ROUND", 1, 2),
        ["Floor"] = new("FLOOR", 1),
        ["Ceiling"] = new("CEIL", 1),
        ["Sqrt"] = new("SQRT", 1),
        ["Power"] = new("POWER", 2),
        ["Sign"] = new("SIGN", 1),
        ["Sin"] = new("SIN", 1),
        ["Cos"] = new("COS", 1),
        ["Tan"] = new("TAN", 1),
        ["Log"] = new("LOG", 1, 2),
        ["Log10"] = new("LOG10", 1),
        ["Exp"] = new("EXP", 1),

        // Date functions (fully supported)
        ["AddDays"] = new("DATEADD('day', {1}, {0})", 2, "DATEADD('day', {1}, {0})"),
        ["AddHours"] = new("DATEADD('hour', {1}, {0})", 2, "DATEADD('hour', {1}, {0})"),
        ["AddMinutes"] = new("DATEADD('minute', {1}, {0})", 2, "DATEADD('minute', {1}, {0})"),
        ["AddSeconds"] = new("DATEADD('second', {1}, {0})", 2, "DATEADD('second', {1}, {0})"),
        ["AddMilliseconds"] = new("DATEADD('millisecond', {1}, {0})", 2, "DATEADD('millisecond', {1}, {0})"),
        ["Year"] = new("YEAR", 1),
        ["Month"] = new("MONTH", 1),
        ["Day"] = new("DAY", 1),
        ["Hour"] = new("HOUR", 1),
        ["Minute"] = new("MINUTE", 1),
        ["Second"] = new("SECOND", 1),
        ["DayOfWeek"] = new("DAY_OF_WEEK", 1),
        ["DayOfYear"] = new("DAY_OF_YEAR", 1),
        ["WeekOfYear"] = new("WEEK_OF_YEAR", 1),

        // Aggregate functions (fully supported)
        ["Sum"] = new("SUM", 1),
        ["Count"] = new("COUNT", 0, 1, true),
        ["Max"] = new("MAX", 1),
        ["Min"] = new("MIN", 1),
        ["Average"] = new("AVG", 1),
        ["LatestByOffset"] = new("LATEST_BY_OFFSET", 1),
        ["EarliestByOffset"] = new("EARLIEST_BY_OFFSET", 1),
        ["CollectList"] = new("COLLECT_LIST", 1),
        ["CollectSet"] = new("COLLECT_SET", 1),
        ["CountDistinct"] = new("COUNT_DISTINCT", 1),
        ["Histogram"] = new("HISTOGRAM", 1),
        ["TopK"] = new("TOPK", 2),
        ["TopKDistinct"] = new("TOPKDISTINCT", 2),

        // Array functions (fully supported)
        ["ArrayLength"] = new("ARRAY_LENGTH", 1),
        ["ArrayContains"] = new("ARRAY_CONTAINS", 2),
        ["ArraySlice"] = new("ARRAY_SLICE", 3),
        ["ArrayJoin"] = new("ARRAY_JOIN", 2),
        ["ArrayDistinct"] = new("ARRAY_DISTINCT", 1),
        ["ArrayExcept"] = new("ARRAY_EXCEPT", 2),
        ["ArrayIntersect"] = new("ARRAY_INTERSECT", 2),
        ["ArrayUnion"] = new("ARRAY_UNION", 2),
        ["ArraySort"] = new("ARRAY_SORT", 1),
        ["ArrayMax"] = new("ARRAY_MAX", 1),
        ["ArrayMin"] = new("ARRAY_MIN", 1),

        // JSON functions (fully supported)
        ["JsonExtractString"] = new("JSON_EXTRACT_STRING", 2),
        ["JsonArrayLength"] = new("JSON_ARRAY_LENGTH", 1),
        ["JsonKeys"] = new("JSON_KEYS", 1),
        ["JsonArrayContains"] = new("JSON_ARRAY_CONTAINS", 2),
        ["JsonConcat"] = new("JSON_CONCAT", 2, int.MaxValue),
        ["JsonRecords"] = new("JSON_RECORDS", 1),

        // Type conversion functions (fully supported)
        ["ToString"] = new("CAST({0} AS VARCHAR)", 1, true, "CAST({0} AS VARCHAR)"),
        ["Parse"] = new("PARSE_{TYPE}", 1, true),
        ["Convert"] = new("CAST({0} AS {TYPE})", 1, true),
        ["ToInt"] = new("CAST({0} AS INTEGER)", 1, "CAST({0} AS INTEGER)"),
        ["ToLong"] = new("CAST({0} AS BIGINT)", 1, "CAST({0} AS BIGINT)"),
        ["ToDouble"] = new("CAST({0} AS DOUBLE)", 1, "CAST({0} AS DOUBLE)"),
        ["ToDecimal"] = new("CAST({0} AS DECIMAL)", 1, "CAST({0} AS DECIMAL)"),

        // Conditional functions
        ["Case"] = new("CASE", 2, int.MaxValue, true),
        ["Coalesce"] = new("COALESCE", 1, int.MaxValue),
        ["IfNull"] = new("IFNULL", 2),
        ["NullIf"] = new("NULLIF", 2),

        // URL functions
        ["UrlExtractHost"] = new("URL_EXTRACT_HOST", 1),
        ["UrlExtractPath"] = new("URL_EXTRACT_PATH", 1),
        ["UrlExtractQuery"] = new("URL_EXTRACT_QUERY", 1),
        ["UrlExtractProtocol"] = new("URL_EXTRACT_PROTOCOL", 1),

        // GEO functions
        ["GeoDistance"] = new("GEO_DISTANCE", 4),
        ["AsGeoJson"] = new("AS_GEOJSON", 2),

        // Encryption functions
        ["Md5"] = new("MD5", 1),
        ["Sha1"] = new("SHA1", 1),
        ["Sha256"] = new("SHA256", 1),

        ["WindowStart"] = new("WINDOWSTART", 0, "WINDOWSTART"),
        ["RowTime"] = new("ROWTIME", 0),
        ["RowKey"] = new("ROWKEY", 0)
    };

    /// <summary>
    /// Retrieve function mapping
    /// </summary>
    public static KsqlFunctionMapping? GetMapping(string methodName)
    {
        return _functionMappings.TryGetValue(methodName, out var mapping) ? mapping : null;
    }

    /// <summary>
    /// Check if function exists
    /// </summary>
    public static bool HasMapping(string methodName)
    {
        return _functionMappings.ContainsKey(methodName);
    }

    /// <summary>
    /// Get all mappings
    /// </summary>
    public static IReadOnlyDictionary<string, KsqlFunctionMapping> GetAllMappings()
    {
        return _functionMappings.AsReadOnly();
    }

    /// <summary>
    /// Get mappings by function category
    /// </summary>
    public static Dictionary<string, List<string>> GetFunctionsByCategory()
    {
        return new Dictionary<string, List<string>>
        {
            ["String"] = ["ToUpper", "ToLower", "Substring", "Length", "Trim", "Replace", "Contains", "StartsWith", "EndsWith", "Split", "Concat", "IndexOf", "PadLeft", "PadRight"],
            ["Math"] = ["Abs", "Round", "Floor", "Ceiling", "Sqrt", "Power", "Sign", "Sin", "Cos", "Tan", "Log", "Log10", "Exp"],
            ["Date"] = ["AddDays", "AddHours", "AddMinutes", "AddSeconds", "AddMilliseconds", "Year", "Month", "Day", "Hour", "Minute", "Second", "DayOfWeek", "DayOfYear", "WeekOfYear"],
            ["Aggregate"] = ["Sum", "Count", "Max", "Min", "Average", "LatestByOffset", "EarliestByOffset", "CollectList", "CollectSet", "CountDistinct", "Histogram", "TopK", "TopKDistinct"],
            ["Array"] = ["ArrayLength", "ArrayContains", "ArraySlice", "ArrayJoin", "ArrayDistinct", "ArrayExcept", "ArrayIntersect", "ArrayUnion", "ArraySort", "ArrayMax", "ArrayMin"],
            ["JSON"] = ["JsonExtractString", "JsonArrayLength", "JsonKeys", "JsonArrayContains", "JsonConcat", "JsonRecords"],
            ["Cast"] = ["ToString", "Parse", "Convert", "ToInt", "ToLong", "ToDouble", "ToDecimal"],
            ["Conditional"] = ["Case", "Coalesce", "IfNull", "NullIf"],
            ["URL"] = ["UrlExtractHost", "UrlExtractPath", "UrlExtractQuery", "UrlExtractProtocol"],
            ["GEO"] = ["GeoDistance", "AsGeoJson"],
            ["Crypto"] = ["Md5", "Sha1", "Sha256"],
            ["Window"] = ["WindowStart", "RowTime", "RowKey"]
        };
    }

    /// <summary>
    /// List of functions requiring special handling
    /// </summary>
    public static HashSet<string> GetSpecialHandlingFunctions()
    {
        return _functionMappings
            .Where(kvp => kvp.Value.RequiresSpecialHandling)
            .Select(kvp => kvp.Key)
            .ToHashSet();
    }

    /// <summary>
    /// Determine whether a function is an aggregate
    /// </summary>
    public static bool IsAggregateFunction(string methodName)
    {
        var aggregateFunctions = GetFunctionsByCategory()["Aggregate"];
        return aggregateFunctions.Contains(methodName);
    }

    /// <summary>
    /// Infer KSQL type from method name
    /// </summary>
    public static string InferTypeFromMethodName(string methodName)
    {
        var name = methodName.ToUpperInvariant();

        return name switch
        {
            "SUM" => "DOUBLE",
            "AVG" => "DOUBLE",
            "COUNT" => "BIGINT",
            "MAX" => "ANY",
            "MIN" => "ANY",
            "TOPK" => "ARRAY",
            "HISTOGRAM" => "MAP",
            "TOINT" or "TOINT32" => "INTEGER",
            "TOLONG" or "TOINT64" => "BIGINT",
            "TODOUBLE" => "DOUBLE",
            "TODECIMAL" => $"DECIMAL({DecimalPrecisionConfig.DecimalPrecision}, {DecimalPrecisionConfig.DecimalScale})",
            "TOSTRING" => "VARCHAR",
            "TOBOOL" or "TOBOOLEAN" => "BOOLEAN",
            _ => "UNKNOWN"
        };
    }

    /// <summary>
    /// Add custom mapping (for extensibility)
    /// </summary>
    public static void RegisterCustomMapping(string methodName, KsqlFunctionMapping mapping)
    {
        _functionMappings[methodName] = mapping;
    }

    /// <summary>
    /// Debug: output all function mappings
    /// </summary>
    public static string GetDebugInfo()
    {
        var categories = GetFunctionsByCategory();
        var result = new System.Text.StringBuilder();

        result.AppendLine("KSQL Function Registry - Supported Functions:");
        result.AppendLine("=" + new string('=', 50));

        foreach (var category in categories)
        {
            result.AppendLine($"\n[{category.Key}] ({category.Value.Count} functions)");
            foreach (var func in category.Value)
            {
                var mapping = GetMapping(func);
                result.AppendLine($"  • {func} → {mapping?.KsqlFunction} (args: {mapping?.MinArgs}-{mapping?.MaxArgs})");
            }
        }

        var known = new HashSet<string>(categories.SelectMany(c => c.Value));
        var extras = _functionMappings.Keys
            .Where(k => !known.Contains(k))
            .OrderBy(k => k)
            .ToList();

        if (extras.Count > 0)
        {
            result.AppendLine($"\n[Custom] ({extras.Count} functions)");
            foreach (var func in extras)
            {
                var mapping = GetMapping(func);
                result.AppendLine($"  • {func} → {mapping?.KsqlFunction} (args: {mapping?.MinArgs}-{mapping?.MaxArgs})");
            }
        }

        return result.ToString();
    }
}
