using Kafka.Ksql.Linq;
using Kafka.Ksql.Linq.Core.Context;
using Microsoft.Extensions.Configuration;

namespace DailyComparisonLib;

public class MyKsqlContext : KafkaKsqlContext
{
    public MyKsqlContext(KafkaContextOptions options) : base(options)
    {
    }

    public static MyKsqlContext FromConfiguration(IConfiguration configuration)
    {
        var options = KafkaContextOptions.FromConfiguration(configuration);
        return new MyKsqlContext(options);
    }

    public static MyKsqlContext FromAppSettings(string path)
    {
        var options = KafkaContextOptions.FromAppSettings(path);
        return new MyKsqlContext(options);
    }
}
