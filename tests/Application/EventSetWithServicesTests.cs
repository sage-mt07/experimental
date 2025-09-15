using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Application;

public class EventSetWithServicesTests
{
    private class TestContext : KsqlContext
    {
        public TestContext() : base(new KsqlDslOptions()) { }

        protected override bool SkipSchemaRegistration => true;
    }

    [Fact(Skip = "Requires full KsqlContext initialization")]
    public void Constructors_CreateInstances()
    {
        var ctx = new TestContext();
        var model = new EntityModel
        {
            EntityType = typeof(TestEntity),
            TopicName = "t",
            AllProperties = typeof(TestEntity).GetProperties(),
            KeyProperties = new[] { typeof(TestEntity).GetProperty(nameof(TestEntity.Id))! }
        };
        var set = new EventSetWithServices<TestEntity>(ctx, model);
        Assert.NotNull(set);
    }
}
