namespace Kafka.Ksql.Linq.Query.Builders;

public enum KeyPathStyle
{
    None,
    Dot,
    Arrow
}

/// <summary>
/// Internal hook for tests and backward compatibility. Regular DSL usage relies on auto-detection
/// which selects Arrow for tables and None for streams. Dot style is available only via explicit override.
/// </summary>
public class RenderOptions
{
    public KeyPathStyle KeyPathStyle { get; set; } = KeyPathStyle.None;
}
