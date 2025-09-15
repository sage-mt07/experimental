using Kafka.Ksql.Linq.Configuration;
using System.Reflection;
using System.Threading.Tasks;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Application;

public class KsqlContextAsyncTests
{
    private class TestContext : KsqlContext
    {
        public TestContext() : base(new KsqlDslOptions()) { }

        protected override bool SkipSchemaRegistration => true;
    }

    [Fact]
    public async Task DisposeAsyncCore_DisposesManagers()
    {
        var ctx = new TestContext();
        await ctx.DisposeAsync();

        var producer = typeof(KsqlContext).GetField("_producerManager", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(ctx)!;
        var consumer = typeof(KsqlContext).GetField("_consumerManager", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(ctx)!;

        var prodDisposed = (bool)producer.GetType().GetField("_disposed", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(producer)!;
        var consDisposed = (bool)consumer.GetType().GetField("_disposed", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(consumer)!;

        Assert.True(prodDisposed);
        Assert.True(consDisposed);
    }
}
