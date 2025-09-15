namespace Kafka.Ksql.Linq.Query.Pipeline;
/// <summary>
/// Query part information.
/// Rationale: manage KSQL statement building blocks in the generator layer.
/// </summary>
internal record QueryPart(
    string Content,
    bool IsRequired,
    int Order = 0)
{
    /// <summary>
    /// Empty query part
    /// </summary>
    public static QueryPart Empty => new(string.Empty, false);

    /// <summary>
    /// Create a required query part
    /// </summary>
    public static QueryPart Required(string content, int order = 0)
    {
        return new QueryPart(content, true, order);
    }

    /// <summary>
    /// Create an optional query part
    /// </summary>
    public static QueryPart Optional(string content, int order = 0)
    {
        return new QueryPart(content, !string.IsNullOrWhiteSpace(content), order);
    }

    /// <summary>
    /// Validity check
    /// </summary>
    public bool IsValid => IsRequired && !string.IsNullOrWhiteSpace(Content);

    /// <summary>
    /// Conditional validity check
    /// </summary>
    public bool IsValidOrOptional => IsValid || (!IsRequired && !string.IsNullOrWhiteSpace(Content));
}
