using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Query.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Ksql.Linq.Query.Pipeline;

/// <summary>
/// Generator base class
/// Rationale: unified implementation base for the Generator layer under separation-of-concerns.
/// Hard constraints: require builder DI, separate context analysis and syntax assembly, full KSQL output responsibility, unified error handling.
/// </summary>
internal abstract class GeneratorBase
{
    /// <summary>
    /// Injected builder instances (read-only)
    /// </summary>
    protected readonly IReadOnlyDictionary<KsqlBuilderType, IKsqlBuilder> Builders;

    /// <summary>
    /// Constructor (requires builder DI)
    /// </summary>
    protected GeneratorBase(IReadOnlyDictionary<KsqlBuilderType, IKsqlBuilder> builders)
    {
        Builders = builders ?? throw new ArgumentNullException(nameof(builders));
        ValidateRequiredBuilders();
    }

    /// <summary>
    /// Ensure required builders exist
    /// </summary>
    protected virtual void ValidateRequiredBuilders()
    {
        // Base class checks only minimal builders
        var requiredBuilders = GetRequiredBuilderTypes();

        foreach (var builderType in requiredBuilders)
        {
            if (!Builders.ContainsKey(builderType))
            {
                throw new InvalidOperationException(
                    $"{GetType().Name} requires {builderType} builder but it was not provided");
            }
        }
    }

    /// <summary>
    /// Define required builder types in derived classes
    /// </summary>
    protected abstract KsqlBuilderType[] GetRequiredBuilderTypes();

    /// <summary>
    /// Retrieve builder (type-safe)
    /// </summary>
    protected IKsqlBuilder GetBuilder(KsqlBuilderType type)
    {
        if (Builders.TryGetValue(type, out var builder))
        {
            return builder;
        }

        throw new InvalidOperationException(
            $"Builder {type} is not available in {GetType().Name}");
    }

    /// <summary>
    /// Check if builder exists
    /// </summary>
    protected bool HasBuilder(KsqlBuilderType type)
    {
        return Builders.ContainsKey(type);
    }

    /// <summary>
    /// Assemble query (unified entry point)
    /// </summary>
    protected static string AssembleQuery(params QueryPart[] parts)
    {
        if (parts == null || parts.Length == 0)
        {
            throw new ArgumentException("Query parts cannot be null or empty");
        }

        // Filter only valid parts
        var validParts = parts
            .Where(p => p != null && p.IsValidOrOptional)
            .OrderBy(p => p.Order)
            .Select(p => p.Content.Trim())
            .Where(content => !string.IsNullOrWhiteSpace(content))
            .ToList();

        if (validParts.Count == 0)
        {
            throw new InvalidOperationException("No valid query parts found");
        }

        var result = string.Join(" ", validParts);

        // Basic syntax checks
        ValidateAssembledQuery(result);

        return result;
    }

    /// <summary>
    /// Basic validation for assembled queries
    /// </summary>
    private static void ValidateAssembledQuery(string query)
    {
        if (string.IsNullOrWhiteSpace(query))
        {
            throw new InvalidOperationException("Assembled query is empty");
        }

        // Basic KSQL syntax checks
        var upperQuery = query.Trim().ToUpper();

        if (!IsValidKsqlQueryStart(upperQuery))
        {
            throw new InvalidOperationException(
                $"Generated query does not start with valid KSQL command: {query}");
        }

        // Balance check (parentheses etc.)
        ValidateQueryBalance(query);
    }

    /// <summary>
    /// Validate start of KSQL query
    /// </summary>
    private static bool IsValidKsqlQueryStart(string upperQuery)
    {
        var validStarts = new[]
        {
            "SELECT", "CREATE STREAM", "CREATE TABLE", "DROP STREAM", "DROP TABLE",
            "INSERT INTO", "SHOW", "DESCRIBE", "EXPLAIN", "LIST", "PRINT"
        };

        return validStarts.Any(start => upperQuery.StartsWith(start));
    }

    /// <summary>
    /// Validate query balance (parentheses, etc.)
    /// </summary>
    private static void ValidateQueryBalance(string query)
    {
        var parenthesesCount = 0;
        var inString = false;

        for (var i = 0; i < query.Length; i++)
        {
            var ch = query[i];
            switch (ch)
            {
                case '\'':
                    // handle '' escape sequence
                    if (i + 1 < query.Length && query[i + 1] == '\'')
                    {
                        i++; // skip escaped quote
                    }
                    else
                    {
                        inString = !inString;
                    }
                    break;

                case '(':
                    if (!inString) parenthesesCount++;
                    break;

                case ')':
                    if (!inString) parenthesesCount--;
                    break;
            }
        }

        if (parenthesesCount != 0)
        {
            throw new InvalidOperationException(
                $"Unbalanced parentheses in query: {query}");
        }

        if (inString)
        {
            throw new InvalidOperationException(
                $"Unclosed string literal in query: {query}");
        }
    }

    /// <summary>
    /// Assemble structured query
    /// </summary>
    protected string AssembleStructuredQuery(QueryStructure structure)
    {
        var result = structure.Validate();
        if (!result.IsValid)
        {
            throw new InvalidOperationException(
                $"Invalid query structure: {result.GetErrorMessage()}");
        }

        var parts = new List<QueryPart>();

        // Prefix based on query type
        parts.Add(CreateQueryPrefix(structure));

        // Add clauses in order
        foreach (var clause in structure.Clauses.OrderBy(c => GetClauseOrder(c.Type)))
        {
            if (clause.IsValid)
            {
                parts.Add(QueryPart.Required(clause.Content, GetClauseOrder(clause.Type)));
            }
        }

        return AssembleQuery(parts.ToArray());
    }

    /// <summary>
    /// Create query prefix
    /// </summary>
    private static QueryPart CreateQueryPrefix(QueryStructure structure)
    {
        return structure.QueryType switch
        {
            "SELECT" => QueryPart.Required("SELECT", 0),
            "CREATE_STREAM_AS" => QueryPart.Required($"CREATE STREAM {structure.TargetObject} AS SELECT", 0),
            "CREATE_TABLE_AS" => QueryPart.Required($"CREATE TABLE {structure.TargetObject} AS SELECT", 0),
            _ => throw new NotSupportedException($"Query type {structure.QueryType} is not supported")
        };
    }

    /// <summary>
    /// Get clause ordering
    /// </summary>
    private static int GetClauseOrder(QueryClauseType clauseType)
    {
        return clauseType switch
        {
            QueryClauseType.Select => 10,
            QueryClauseType.From => 20,
            QueryClauseType.Join => 30,
            QueryClauseType.Where => 40,
            QueryClauseType.GroupBy => 50,
            QueryClauseType.Having => 70,
            QueryClauseType.OrderBy => 80,
            QueryClauseType.Limit => 90,
            _ => 999
        };
    }

    /// <summary>
    /// Map C# types to KSQL types
    /// </summary>
    protected static string MapToKSqlType(Type type)
    {
        var underlyingType = Nullable.GetUnderlyingType(type) ?? type;

        return underlyingType switch
        {
            Type t when t == typeof(int) => "INTEGER",
            Type t when t == typeof(short) => "INTEGER",
            Type t when t == typeof(long) => "BIGINT",
            Type t when t == typeof(double) => "DOUBLE",
            Type t when t == typeof(float) => "DOUBLE",
            Type t when t == typeof(decimal) => $"DECIMAL({DecimalPrecisionConfig.DecimalPrecision}, {DecimalPrecisionConfig.DecimalScale})",
            Type t when t == typeof(string) => "VARCHAR",
            Type t when t == typeof(char) => "VARCHAR",
            Type t when t == typeof(bool) => "BOOLEAN",
            Type t when t == typeof(DateTime) => "TIMESTAMP",
            Type t when t == typeof(DateTimeOffset) => "TIMESTAMP",
            Type t when t == typeof(Guid) => "VARCHAR",
            Type t when t == typeof(byte[]) => "BYTES",
            _ when underlyingType.IsEnum => throw new NotSupportedException($"Type '{underlyingType.Name}' is not supported."),
            _ when !underlyingType.IsPrimitive &&
                   underlyingType != typeof(string) &&
                   underlyingType != typeof(char) &&
                   underlyingType != typeof(Guid) &&
                   underlyingType != typeof(byte[]) =>
                throw new NotSupportedException($"Type '{underlyingType.Name}' is not supported."),
            _ => throw new NotSupportedException($"Type '{underlyingType.Name}' is not supported.")
        };
    }

    /// <summary>
    /// Unified error-handling method
    /// </summary>
    protected string HandleGenerationError(string operation, System.Exception exception, string? context = null)
    {
        var errorMessage = $"{GetType().Name} failed during {operation}";

        if (!string.IsNullOrEmpty(context))
        {
            errorMessage += $" (Context: {context})";
        }

        errorMessage += $": {exception.Message}";

        // Add debug information in development environment
        if (IsDebugMode())
        {
            errorMessage += $"\nStack Trace: {exception.StackTrace}";
        }

        throw new InvalidOperationException(errorMessage, exception);
    }

    /// <summary>
    /// Safe wrapper for builder invocation
    /// </summary>
    protected string SafeCallBuilder(KsqlBuilderType builderType, System.Linq.Expressions.Expression expression, string operation)
    {
        try
        {
            var builder = GetBuilder(builderType);
            return builder.Build(expression);
        }
        catch (Exception ex)
        {
            return HandleGenerationError($"{operation} using {builderType} builder", ex, expression.ToString());
        }
    }

    /// <summary>
    /// Conditional builder invocation
    /// </summary>
    protected string? TryCallBuilder(KsqlBuilderType builderType, System.Linq.Expressions.Expression? expression)
    {
        if (expression == null || !HasBuilder(builderType))
        {
            return null;
        }

        try
        {
            var builder = GetBuilder(builderType);
            return builder.Build(expression);
        }
        catch
        {
            // Ignore exception and return null (best-effort optional processing)
            return null;
        }
    }

    /// <summary>
    /// Determine debug mode
    /// </summary>
    private static bool IsDebugMode()
    {
#if DEBUG
        return true;
#else
        return Environment.GetEnvironmentVariable("KSQL_DEBUG") == "true";
#endif
    }

    /// <summary>
    /// Retrieve generator info (for debugging)
    /// </summary>
    protected virtual string GetGeneratorInfo()
    {
        var builderTypes = string.Join(", ", Builders.Keys);
        return $"Generator: {GetType().Name}, Available Builders: [{builderTypes}]";
    }

    /// <summary>
    /// Common post-processing (e.g., EMIT CHANGES)
    /// </summary>
    protected string ApplyQueryPostProcessing(string baseQuery, QueryAssemblyContext context)
    {
        var query = baseQuery.Trim();
        var upper = query.ToUpper();

        // Error if GROUP BY is specified in Pull or TABLE queries
        if (upper.Contains("GROUP BY") && (context.IsPullQuery || context.IsTableQuery))
        {
            throw new InvalidOperationException(
                "GROUP BY is not supported in pull or table queries. Use a push query with EMIT CHANGES instead.");
        }

        // Treat TABLE queries as pull queries and do not append EMIT CHANGES
        if (!context.IsTableQuery)
        {
            // When GROUP BY is present, always treat as a push query and append EMIT CHANGES
            if (upper.Contains("GROUP BY") && !upper.Contains("EMIT CHANGES"))
            {
                query += " EMIT CHANGES";
            }
            else if (!context.IsPullQuery && !upper.Contains("EMIT CHANGES"))
            {
                query += " EMIT CHANGES";
            }
        }

        // Additional processing based on metadata
        if (context.HasMetadata("WITH_OPTIONS"))
        {
            var withOptions = context.GetMetadata<string>("WITH_OPTIONS");
            if (!string.IsNullOrEmpty(withOptions))
            {
                query += $" WITH ({withOptions})";
            }
        }

        if (!query.TrimEnd().EndsWith(";"))
        {
            query += ";";
        }

        return query;
    }

    /// <summary>
    /// Apply query optimization hints
    /// </summary>
    protected virtual string ApplyOptimizationHints(string query, QueryAssemblyContext context)
    {
        // No operation in base class (implemented in derived classes)
        return query;
    }

    /// <summary>
    /// ToString implementation (for debugging)
    /// </summary>
    public override string ToString()
    {
        return GetGeneratorInfo();
    }
}
