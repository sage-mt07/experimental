using System.Collections.Generic;

namespace Kafka.Ksql.Linq.Query.Builders;
/// <summary>
/// JOIN information class
/// </summary>
internal class JoinInfo
{
    public string OuterType { get; set; } = string.Empty;
    public string InnerType { get; set; } = string.Empty;
    public List<string> OuterKeys { get; set; } = new();
    public List<string> InnerKeys { get; set; } = new();
    public List<string> Projections { get; set; } = new();
    public string OuterAlias { get; set; } = "o";
    public string InnerAlias { get; set; } = "i";
}
