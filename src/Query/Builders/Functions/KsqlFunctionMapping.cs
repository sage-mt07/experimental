using System;

namespace Kafka.Ksql.Linq.Query.Builders.Functions;

/// <summary>
/// KSQL function mapping definition
/// Design rationale: defines conversion rules from C# methods to KSQL functions
/// </summary>
internal record KsqlFunctionMapping(
    string KsqlFunction,
    int MinArgs,
    int MaxArgs = int.MaxValue,
    bool RequiresSpecialHandling = false,
    string? CustomTemplate = null)
{
    /// <summary>
    /// Constructor with fixed argument count
    /// </summary>
    public KsqlFunctionMapping(string ksqlFunction, int exactArgs)
        : this(ksqlFunction, exactArgs, exactArgs)
    {
    }

    /// <summary>
    /// Constructor with custom template
    /// </summary>
    public KsqlFunctionMapping(string ksqlFunction, int minArgs, int maxArgs, string customTemplate)
        : this(ksqlFunction, minArgs, maxArgs, false, customTemplate)
    {
    }
    /// <summary>
    /// Constructor with custom template (fixed argument count)
    /// </summary>
    public KsqlFunctionMapping(string ksqlFunction, int exactArgs, string customTemplate)
        : this(ksqlFunction, exactArgs, exactArgs, false, customTemplate)
    {
    }

    /// <summary>
    /// Constructor with special-handling flag (fixed argument count)
    /// </summary>
    public KsqlFunctionMapping(string ksqlFunction, int exactArgs, bool requiresSpecialHandling)
        : this(ksqlFunction, exactArgs, exactArgs, requiresSpecialHandling)
    {
    }

    /// <summary>
    /// Constructor with special-handling flag and custom template (fixed argument count)
    /// </summary>
    public KsqlFunctionMapping(string ksqlFunction, int exactArgs, bool requiresSpecialHandling, string? customTemplate)
        : this(ksqlFunction, exactArgs, exactArgs, requiresSpecialHandling, customTemplate)
    {
    }
    /// <summary>
    /// Validate argument count
    /// </summary>
    public bool IsValidArgCount(int argCount)
    {
        return argCount >= MinArgs && argCount <= MaxArgs;
    }

    /// <summary>
    /// Check template applicability
    /// </summary>
    public bool HasCustomTemplate => !string.IsNullOrEmpty(CustomTemplate);

    /// <summary>
    /// Generate standard function invocation format
    /// </summary>
    public string GenerateStandardCall(params string[] args)
    {
        if (!IsValidArgCount(args.Length))
        {
            throw new ArgumentException($"Invalid argument count for {KsqlFunction}. Expected {MinArgs}-{MaxArgs}, got {args.Length}");
        }

        if (HasCustomTemplate)
        {
            return ApplyCustomTemplate(args);
        }

        return $"{KsqlFunction}({string.Join(", ", args)})";
    }

    /// <summary>
    /// Apply custom template
    /// </summary>
    private string ApplyCustomTemplate(string[] args)
    {
        var result = CustomTemplate!;
        for (int i = 0; i < args.Length; i++)
        {
            result = result.Replace($"{{{i}}}", args[i]);
        }
        return result;
    }
}
