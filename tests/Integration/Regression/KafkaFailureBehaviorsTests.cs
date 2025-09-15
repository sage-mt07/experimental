using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Core.Configuration;
using System;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Regression;

// Note: These tests are placeholders to capture expected behaviors without requiring
// a live Kafka/Schema Registry. They are skipped by default and serve as a
// spec for failure-mode handling validated by physical tests.
public class KafkaFailureBehaviorsTests
{
    [KsqlTopic("orders")]
    public class Order
    {
        public int Id { get; set; }
        public double Amount { get; set; }
    }

    public class OrderContext : KsqlContext
    {
        public OrderContext(KsqlDslOptions options) : base(options) { }
        protected override void OnModelCreating(IModelBuilder modelBuilder)
            => modelBuilder.Entity<Order>();
        protected override bool SkipSchemaRegistration => true;
    }

    [Fact(Skip = "Physical env only: documents expected exception surface for keyless entity produce when Kafka is down")]
    public async Task Keyless_Entity_Produce_Should_Not_Throw_ArgumentNullException()
    {
        // Arrange
        var options = new KsqlDslOptions
        {
            Common = new CommonSection { BootstrapServers = "localhost:9092" },
            SchemaRegistry = new SchemaRegistrySection { Url = "http://localhost:8081" }
        };
        await using var ctx = new OrderContext(options);

        // Act
        var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
            await ctx.Set<Order>().AddAsync(new Order { Id = 1, Amount = 100 }, cancellationToken: CancellationToken.None));

        // Assert: should surface Kafka/serialization-related errors, not ArgumentNullException
        Assert.False(ex is ArgumentNullException, $"Unexpected ArgumentNullException: {ex}");
    }
}
