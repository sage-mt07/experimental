using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Application;

public class ConfigureModelEventSetRegistrationTests
{
    private class Sample
    {
        public int Id { get; set; }
    }

    private class TestContext : KsqlContext
    {
        public EventSet<Sample> Samples { get; set; } = null!;

        public TestContext() : base(new KsqlDslOptions()) { }

        protected override bool SkipSchemaRegistration => true;

        protected override void OnModelCreating(IModelBuilder modelBuilder)
        {
            // No explicit registration; rely on automatic EventSet discovery
        }
    }

    [Fact]
    public void EventSetProperty_IsConnectedToModelBuilder()
    {
        var ctx = new TestContext();
        Assert.NotNull(ctx.Samples);
        var models = ctx.GetEntityModels();
        Assert.True(models.ContainsKey(typeof(Sample)));
    }
}
