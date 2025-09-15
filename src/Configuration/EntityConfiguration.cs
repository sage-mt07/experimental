namespace Kafka.Ksql.Linq.Configuration
{
    public class EntityConfiguration
    {
        public string Entity { get; set; } = string.Empty;
        public string SourceTopic { get; set; } = string.Empty;
        public bool EnableCache { get; set; } = false;
        public string? StoreName { get; set; }
        public string? BaseDirectory { get; set; }
    }
}
