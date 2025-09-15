using Confluent.SchemaRegistry;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
namespace Kafka.Ksql.Linq.Application;
public class KsqlContextBuilder
{
    private readonly KsqlContextOptions _options = new();

    public static KsqlContextBuilder Create()
    {
        return new KsqlContextBuilder();
    }

    public KsqlContextBuilder UseConfiguration(IConfiguration configuration)
    {
        if (configuration == null) throw new ArgumentNullException(nameof(configuration));
        _options.Configuration = configuration;
        return this;
    }

    public KsqlContextBuilder UseSchemaRegistry(string url)
    {
        _options.UseSchemaRegistry(url);
        return this;
    }

    public KsqlContextBuilder UseSchemaRegistry(SchemaRegistryConfig config)
    {
        _options.UseSchemaRegistry(config);
        return this;
    }

    public KsqlContextBuilder UseSchemaRegistry(ISchemaRegistryClient client)
    {
        _options.SchemaRegistryClient = client;
        return this;
    }

    public KsqlContextBuilder EnableLogging(ILoggerFactory loggerFactory)
    {
        _options.EnableLogging(loggerFactory);
        return this;
    }

    public KsqlContextBuilder ConfigureValidation(
        bool autoRegister = true,
        bool failOnErrors = true,
        bool enablePreWarming = true)
    {
        _options.ConfigureValidation(autoRegister, failOnErrors, enablePreWarming);
        return this;
    }

    public KsqlContextBuilder WithTimeouts(TimeSpan schemaRegistrationTimeout)
    {
        _options.WithTimeouts(schemaRegistrationTimeout);
        return this;
    }


    public KsqlContextOptions Build()
    {
        _options.Validate();
        return _options;
    }

    public T BuildContext<T>() where T : KsqlContext
    {
        var options = Build();

        // Prefer constructor with KsqlContextOptions if available
        var ctor = typeof(T).GetConstructor(new[] { typeof(KsqlContextOptions) });
        if (ctor != null)
        {
            return (T)ctor.Invoke(new object[] { options });
        }

        // Fallback to parameterless constructor
        ctor = typeof(T).GetConstructor(Type.EmptyTypes);
        if (ctor != null)
        {
            return (T)ctor.Invoke(null);
        }

        // Last resort: try original Activator (may throw)
        return (T)Activator.CreateInstance(typeof(T), options)!;
    }
}
