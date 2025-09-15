using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using System;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Application;

public class WaitForReadyTests
{
    private class FakeHandler : HttpMessageHandler
    {
        private readonly string _content;
        public FakeHandler(string content) { _content = content; }
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var resp = new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(_content, Encoding.UTF8, "application/json")
            };
            return Task.FromResult(resp);
        }
    }

    private class ReadyContext : KsqlContext
    {
        public ReadyContext(string json) : base(new KsqlDslOptions())
        {
            var handler = new FakeHandler(json);
            var client = new HttpClient(handler) { BaseAddress = new Uri("http://localhost") };
            var field = typeof(KsqlContext).GetField("_ksqlDbClient", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
            field.SetValue(this, new Lazy<HttpClient>(() => client));
        }
        protected override void OnModelCreating(IModelBuilder builder)
        {
            builder.Entity<TestEntity>();
        }
        protected override bool SkipSchemaRegistration => true;
    }

    private class TestEntity { public int Id { get; set; } }

    [Fact(Skip = "Requires KsqlDb client")]
    public async Task WaitForEntityReadyAsync_ReturnsWhenExists()
    {
        var json = "[{\"@type\":\"streams\",\"streams\":[{\"name\":\"TESTENTITY\"}]}]";
        await using var ctx = new ReadyContext(json);
        await ctx.WaitForEntityReadyAsync<TestEntity>(TimeSpan.FromSeconds(1));
    }

    [Fact(Skip = "Requires KsqlDb client")]
    public async Task WaitForEntityReadyAsync_ThrowsOnTimeout()
    {
        var json = "[{\"@type\":\"streams\",\"streams\":[]}]";
        await using var ctx = new ReadyContext(json);
        await Assert.ThrowsAsync<TimeoutException>(() => ctx.WaitForEntityReadyAsync<TestEntity>(TimeSpan.FromMilliseconds(200)));
    }
}
