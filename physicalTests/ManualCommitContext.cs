using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;

#nullable enable

namespace Kafka.Ksql.Linq.Tests.Integration;

internal class ManualCommitContext : KsqlContext
{
    public ManualCommitContext(KsqlDslOptions options) : base(options) { }

    protected override bool SkipSchemaRegistration => true;

    public EventSet<Sample> Samples { get; private set; } = null!;

    protected override void OnModelCreating(IModelBuilder modelBuilder)
        => modelBuilder.Entity<Sample>();

    [KsqlTopic("manual_commit")]
    internal class Sample
    {
        public int Id { get; set; }
    }
}
