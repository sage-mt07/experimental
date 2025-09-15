using System.Collections.Generic;
using System.Linq;

namespace Kafka.Ksql.Linq.Query.Pipeline;

/// <summary>
/// Validation result
/// </summary>
internal record ValidationResult(bool IsValid, List<string> Errors)
{
    /// <summary>
    /// Success result
    /// </summary>
    public static ValidationResult Success => new(true, new List<string>());

    /// <summary>
    /// Create failure result
    /// </summary>
    public static ValidationResult Failure(params string[] errors)
    {
        return new ValidationResult(false, errors.ToList());
    }

    /// <summary>
    /// Combine error messages
    /// </summary>
    public string GetErrorMessage()
    {
        return Errors.Count > 0 ? string.Join("; ", Errors) : string.Empty;
    }
}
