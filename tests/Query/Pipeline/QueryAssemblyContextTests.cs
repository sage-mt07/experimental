using Kafka.Ksql.Linq.Query.Pipeline;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Pipeline;

public class QueryAssemblyContextTests
{
    [Fact]
    public void WithMetadata_AddsEntryAndReturnsNewInstance()
    {
        var ctx = new QueryAssemblyContext("base");
        var ctx2 = ctx.WithMetadata("k", 1);

        Assert.NotSame(ctx, ctx2);
        Assert.False(ctx.HasMetadata("k"));
        Assert.True(ctx2.HasMetadata("k"));
        Assert.Equal(1, ctx2.GetMetadata<int>("k"));
    }

    [Fact]
    public void WithExecutionMode_UpdatesFlags()
    {
        var ctx = new QueryAssemblyContext("base");
        var updated = ctx.WithExecutionMode(QueryExecutionMode.PushQuery);

        Assert.Equal(QueryExecutionMode.PullQuery, ctx.ExecutionMode);
        Assert.True(ctx.IsPullQuery);

        Assert.Equal(QueryExecutionMode.PushQuery, updated.ExecutionMode);
        Assert.False(updated.IsPullQuery);
    }

    [Fact]
    public void Copy_CreatesDeepCopyOfMetadata()
    {
        var ctx = new QueryAssemblyContext("b").WithMetadata("a", 1);
        var copy = ctx.Copy();

        var ctxMeta = ctx.GetMetadata<int>("a");
        var copyMeta = copy.GetMetadata<int>("a");
        Assert.Equal(ctxMeta, copyMeta);

        var newCopy = copy.WithMetadata("b", 2);
        Assert.False(ctx.HasMetadata("b"));
        Assert.True(newCopy.HasMetadata("b"));
    }
}
