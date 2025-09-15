using System.Collections.Generic;
using System.Linq;

namespace Kafka.Ksql.Linq.Query.Pipeline;
internal record QueryAssemblyContext(
    string BaseObjectName,
    QueryExecutionMode ExecutionMode,
    bool IsPullQuery,
    Dictionary<string, object> Metadata,
    bool IsTableQuery = false)
{
    /// <summary>
    /// Default constructor
    /// </summary>
    public QueryAssemblyContext(string baseObjectName, QueryExecutionMode executionMode, bool isTableQuery = false)
        : this(baseObjectName, executionMode, executionMode == QueryExecutionMode.PullQuery, new Dictionary<string, object>(), isTableQuery)
    {
    }

    /// <summary>
    /// Simple constructor
    /// </summary>
    public QueryAssemblyContext(string baseObjectName, bool isPullQuery = true, bool isTableQuery = false)
        : this(baseObjectName, isPullQuery ? QueryExecutionMode.PullQuery : QueryExecutionMode.PushQuery, isPullQuery, new Dictionary<string, object>(), isTableQuery)
    {
    }

    /// <summary>
    /// Add metadata
    /// </summary>
    public QueryAssemblyContext WithMetadata(string key, object value)
    {
        var newMetadata = new Dictionary<string, object>(Metadata) { [key] = value };
        return this with { Metadata = newMetadata };
    }

    /// <summary>
    /// Change execution mode
    /// </summary>
    public QueryAssemblyContext WithExecutionMode(QueryExecutionMode mode)
    {
        return this with
        {
            ExecutionMode = mode,
            IsPullQuery = mode == QueryExecutionMode.PullQuery
        };
    }

    /// <summary>
    /// Change base object
    /// </summary>
    public QueryAssemblyContext WithBaseObject(string baseObjectName)
    {
        return this with { BaseObjectName = baseObjectName };
    }

    /// <summary>
    /// Set TABLE query flag
    /// </summary>
    public QueryAssemblyContext WithTableQuery(bool isTable)
    {
        return this with { IsTableQuery = isTable };
    }

    /// <summary>
    /// Get metadata (type-safe)
    /// </summary>
    public T? GetMetadata<T>(string key)
    {
        if (Metadata.TryGetValue(key, out var value) && value is T typedValue)
        {
            return typedValue;
        }
        return default;
    }

    /// <summary>
    /// Check for metadata presence
    /// </summary>
    public bool HasMetadata(string key)
    {
        return Metadata.ContainsKey(key);
    }

    /// <summary>
    /// Clone context information
    /// </summary>
    public QueryAssemblyContext Copy()
    {
        return this with { Metadata = new Dictionary<string, object>(Metadata) };
    }

    /// <summary>
    /// Debug information string
    /// </summary>
    public string GetDebugInfo()
    {
        var metadataInfo = Metadata.Count > 0
            ? string.Join(", ", Metadata.Select(kvp => $"{kvp.Key}={kvp.Value}"))
            : "none";

        return $"BaseObject: {BaseObjectName}, " +
               $"Mode: {ExecutionMode}, " +
               $"IsPull: {IsPullQuery}, " +
               $"IsTable: {IsTableQuery}, " +
               $"Metadata: [{metadataInfo}]";
    }
}
