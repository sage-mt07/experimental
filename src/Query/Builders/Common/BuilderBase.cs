using Kafka.Ksql.Linq.Query.Abstractions;
using System;
using System.Linq.Expressions;

namespace Kafka.Ksql.Linq.Query.Builders.Common;

/// <summary>
/// Base class for builders.
/// Rationale: centralize common constraints/validation under separation-of-concerns design.
/// Hard constraints: readonly fields only, static methods preferred, no external refs beyond Expression, side‑effect free.
/// </summary>
internal abstract class BuilderBase : IKsqlBuilder
{
    /// <summary>
    /// Builder kind (must be implemented by derived classes)
    /// </summary>
    public abstract KsqlBuilderType BuilderType { get; }

    /// <summary>
    /// Build a KSQL statement from an expression (public API)
    /// </summary>
    /// <param name="expression">Target expression tree</param>
    /// <returns>KSQL statement string</returns>
    public string Build(Expression expression)
    {
        // Run common validations
        ValidateInput(expression);

        try
        {
            // Invoke derived implementation
            var result = BuildInternal(expression);

            // Validate the result
            ValidateOutput(result);

            return result;
        }
        catch (Exception ex) when (!(ex is ArgumentException || ex is InvalidOperationException))
        {
            // Convert unexpected errors into a more specific error
            throw new InvalidOperationException(
                $"Failed to build {BuilderType} clause from expression. " +
                $"Expression: {expression}. " +
                $"Error: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// Core implementation for derived classes (protected method)
    /// </summary>
    /// <param name="expression">Validated expression tree</param>
    /// <returns>KSQL statement string</returns>
    protected abstract string BuildInternal(Expression expression);
    /// <summary>
    /// Define required builder types (override in derived classes)
    /// </summary>
    /// <returns>Array of dependent builder types</returns>
    protected virtual KsqlBuilderType[] GetRequiredBuilderTypes()
    {
        return Array.Empty<KsqlBuilderType>(); // No dependencies by default
    }
    /// <summary>
    /// Input validation (common)
    /// </summary>
    private void ValidateInput(Expression expression)
    {
        if (expression == null)
        {
            throw new ArgumentNullException(nameof(expression),
                $"{BuilderType} builder requires a non-null expression");
        }

        // Run common validation
        BuilderValidation.ValidateExpression(expression);

        // Builder-specific validation
        ValidateBuilderSpecific(expression);
    }

    /// <summary>
    /// Output validation (common)
    /// </summary>
    private void ValidateOutput(string result)
    {
        if (string.IsNullOrWhiteSpace(result))
        {
            throw new InvalidOperationException(
                $"{BuilderType} builder produced empty result. " +
                "This indicates an issue with the expression processing logic.");
        }

        // Basic SQL-injection safety check
        ValidateBasicSqlSafety(result);
    }

    /// <summary>
    /// Builder-specific validation (override as needed)
    /// </summary>
    /// <param name="expression">Expression to validate</param>
    protected virtual void ValidateBuilderSpecific(Expression expression)
    {
        // Default: no-op (override in derived classes if needed)
    }

    /// <summary>
    /// Basic SQL safety checks
    /// </summary>
    private static void ValidateBasicSqlSafety(string result)
    {
        // Check basic dangerous patterns
        var dangerousPatterns = new[]
        {
            "--", "/*", "*/", ";--", "';", "DROP", "DELETE", "INSERT", "UPDATE",
            "EXEC", "EXECUTE", "sp_", "xp_", "UNION", "SCRIPT"
        };

        var upperResult = result.ToUpper();
        foreach (var pattern in dangerousPatterns)
        {
            if (upperResult.Contains(pattern))
            {
                throw new InvalidOperationException(
                    $"Generated SQL contains potentially dangerous pattern: '{pattern}'. " +
                    $"Generated SQL: {result}");
            }
        }
    }

    /// <summary>
    /// Common helper: safely extract MemberExpression
    /// </summary>
    protected static MemberExpression? SafeExtractMember(Expression expression)
    {
        return BuilderValidation.ExtractMemberExpression(expression);
    }

    /// <summary>
    /// Common helper: safely extract lambda body
    /// </summary>
    protected static Expression? SafeExtractLambdaBody(Expression expression)
    {
        return BuilderValidation.ExtractLambdaBody(expression);
    }

    /// <summary>
    /// Common helper: null-safe string conversion
    /// </summary>
    protected static string SafeToString(object? value)
    {
        return BuilderValidation.SafeToString(value);
    }

    /// <summary>
    /// Common helper: check expression type
    /// </summary>
    protected static bool IsExpressionType<T>(Expression expression) where T : Expression
    {
        return expression is T;
    }

    /// <summary>
    /// Common helper: extract method name
    /// </summary>
    protected static string? ExtractMethodName(Expression expression)
    {
        return expression is MethodCallExpression methodCall ? methodCall.Method.Name : null;
    }

    /// <summary>
    /// Error message generation helper
    /// </summary>
    protected string CreateErrorMessage(string operation, Expression expression, Exception? innerException = null)
    {
        var message = $"{BuilderType} builder failed during {operation}. " +
                     $"Expression type: {expression.GetType().Name}. " +
                     $"Expression: {expression}";

        if (innerException != null)
        {
            message += $". Inner error: {innerException.Message}";
        }

        return message;
    }

    /// <summary>
    /// Generate debug information (used during development)
    /// </summary>
    protected virtual string GetDebugInfo(Expression expression)
    {
        return $"Builder: {GetType().Name}, " +
               $"Type: {BuilderType}, " +
               $"Expression: {expression.GetType().Name}, " +
               $"NodeType: {expression.NodeType}";
    }

    /// <summary>
    /// ToString implementation (for debugging)
    /// </summary>
    public override string ToString()
    {
        return $"{GetType().Name}[{BuilderType}]";
    }
}
