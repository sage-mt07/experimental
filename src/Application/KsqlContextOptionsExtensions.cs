using Confluent.SchemaRegistry;
using Microsoft.Extensions.Logging;
using System;
namespace Kafka.Ksql.Linq.Application;
public static class KsqlContextOptionsExtensions
{
    public static KsqlContextOptions UseSchemaRegistry(
        this KsqlContextOptions options,
        string url)
    {
        var config = new SchemaRegistryConfig { Url = url };
        options.SchemaRegistryClient = new CachedSchemaRegistryClient(config);
        return options;
    }

    public static KsqlContextOptions UseSchemaRegistry(
        this KsqlContextOptions options,
        SchemaRegistryConfig config)
    {
        options.SchemaRegistryClient = new CachedSchemaRegistryClient(config);
        return options;
    }

    public static KsqlContextOptions EnableLogging(
        this KsqlContextOptions options,
        ILoggerFactory loggerFactory)
    {
        options.LoggerFactory = loggerFactory;
        return options;
    }

    public static KsqlContextOptions ConfigureValidation(
        this KsqlContextOptions options,
        bool autoRegister = true,
        bool failOnErrors = true,
        bool enablePreWarming = true)
    {
        options.AutoRegisterSchemas = autoRegister;
        options.FailOnInitializationErrors = failOnErrors;
        options.FailOnSchemaErrors = failOnErrors;
        options.EnableCachePreWarming = enablePreWarming;
        return options;
    }

    public static KsqlContextOptions WithTimeouts(
        this KsqlContextOptions options,
        TimeSpan schemaRegistrationTimeout)
    {
        options.SchemaRegistrationTimeout = schemaRegistrationTimeout;
        return options;
    }

}
