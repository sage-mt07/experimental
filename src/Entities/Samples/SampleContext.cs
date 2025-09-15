using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Entities.Samples.Models;
using System;

namespace Kafka.Ksql.Linq.Entities.Samples;

/// <summary>
/// Minimal context demonstrating OnModelCreating based registration.
/// EntitySet creation is not implemented in this sample.
/// </summary>
public class SampleContext : KsqlContext
{
    public SampleContext() : base(new KsqlDslOptions()) { }
    protected override IEntitySet<T> CreateEntitySet<T>(EntityModel entityModel)
    {
        throw new NotImplementedException();
    }

    protected override void OnModelCreating(IModelBuilder modelBuilder)
    {
        modelBuilder.Entity<User>(readOnly: true);

        modelBuilder.Entity<Product>();

        modelBuilder.Entity<Order>(writeOnly: true);
    }
}
