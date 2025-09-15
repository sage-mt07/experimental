using Kafka.Ksql.Linq.Core.Modeling;
using Kafka.Ksql.Linq.Entities.Samples.Models;
using Microsoft.Extensions.DependencyInjection;

namespace Kafka.Ksql.Linq.Entities.Samples;

public static class SampleModelRegistration
{
    /// <summary>
    /// Registers sample models into the DI container.
    /// </summary>
    public static IServiceCollection AddSampleModels(this IServiceCollection services)
    {
        // create a model builder and register sample entities
        var builder = new ModelBuilder();

        builder.Entity<User>();

        builder.Entity<Product>();

        builder.Entity<Order>();

        services.AddSingleton(builder);

        return services;
    }
}
