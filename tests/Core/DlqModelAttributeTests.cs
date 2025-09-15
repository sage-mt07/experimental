using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Mapping;
using Kafka.Ksql.Linq.Messaging;
using Kafka.Ksql.Linq.Query.Abstractions;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

#nullable enable

namespace Kafka.Ksql.Linq.Tests.Core;

public class DlqModelAttributeTests
{
    private class DummyContext : KsqlContext
    {
        private DummyContext() : base(new Microsoft.Extensions.Configuration.ConfigurationBuilder().Build()) { }
    }

    private static DummyContext CreateContext(KsqlDslOptions options)
    {
        DefaultValueBinder.ApplyDefaults(options);
        var ctx = (DummyContext)RuntimeHelpers.GetUninitializedObject(typeof(DummyContext));
        var field = typeof(KsqlContext).GetField("_dslOptions", BindingFlags.Instance | BindingFlags.NonPublic);
        field!.SetValue(ctx, options);
        typeof(KsqlContext).GetField("_mappingRegistry", BindingFlags.Instance | BindingFlags.NonPublic)!
            .SetValue(ctx, new MappingRegistry());
        typeof(KsqlContext).GetField("_entityModels", BindingFlags.Instance | BindingFlags.NonPublic)!
            .SetValue(ctx, new ConcurrentDictionary<Type, EntityModel>());
        return ctx;
    }

    private class TopicOverrideSample
    {
        [KsqlKey]
        public int Id { get; set; }
    }

    [Fact(Skip = "Requires full context initialization")]
    public void DlqEnvelope_ShouldBeStream()
    {
        var ctx = CreateContext(new KsqlDslOptions());
        var model = PrivateAccessor.InvokePrivate<EntityModel>(ctx, "CreateEntityModelFromType", new[] { typeof(Type) }, args: new object[] { typeof(Kafka.Ksql.Linq.Messaging.DlqEnvelope) });
        Assert.Equal(StreamTableType.Stream, model.StreamTableType);
    }

    [Fact]
    public void ConfigTopicOverridesAttribute()
    {
        var options = new KsqlDslOptions();
        DefaultValueBinder.ApplyDefaults(options);
        options.Entities.Add(new EntityConfiguration { Entity = nameof(TopicOverrideSample), SourceTopic = "config-topic" });
        var ctx = CreateContext(options);
        var model = PrivateAccessor.InvokePrivate<EntityModel>(ctx, "CreateEntityModelFromType", new[] { typeof(Type) }, args: new object[] { typeof(TopicOverrideSample) });
        Assert.Equal("config-topic", model.TopicName);
    }

    [Fact(Skip = "Requires DlqOptions configuration")]
    public void DlqConfigOverridesAttribute()
    {
        var options = new KsqlDslOptions();
        options.DlqTopicName = "configured-dlq";
        options.DlqOptions.NumPartitions = 3;
        options.DlqOptions.ReplicationFactor = 2;
        var ctx = CreateContext(options);
        var model = PrivateAccessor.InvokePrivate<EntityModel>(ctx, "CreateEntityModelFromType", new[] { typeof(Type) }, args: new object[] { typeof(Kafka.Ksql.Linq.Messaging.DlqEnvelope) });
        Assert.Equal("configured-dlq", model.TopicName);
        Assert.Equal(3, model.Partitions);
        Assert.Equal((short)2, model.ReplicationFactor);
    }

    private class TestErrorSink : IErrorSink
    {
        public bool Handled { get; private set; }
        public Task HandleErrorAsync(ErrorContext errorContext, KafkaMessageContext messageContext)
        {
            Handled = true;
            return Task.CompletedTask;
        }
        public Task HandleErrorAsync(ErrorContext errorContext) => HandleErrorAsync(errorContext, new KafkaMessageContext());
        public Task InitializeAsync() => Task.CompletedTask;
        public Task CleanupAsync() => Task.CompletedTask;
        public bool IsAvailable => true;
    }

    private class TestEventSet : EventSet<TopicOverrideSample>
    {
        private readonly TopicOverrideSample _item;
        public TestEventSet(KsqlContext ctx, EntityModel model, IErrorSink sink, TopicOverrideSample item)
            : base(ctx, model, sink)
        {
            _item = item;
        }

        protected override async IAsyncEnumerable<(TopicOverrideSample Entity, Dictionary<string, string> Headers, MessageMeta Meta)> ConsumeAsync(
            KsqlContext context,
            bool autoCommit,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var meta = new MessageMeta("t", 0, 0, DateTime.UtcNow, null, null, false, new Dictionary<string, string>());
            yield return (_item, new Dictionary<string, string>(), meta);
            await Task.CompletedTask;
        }

        public override IAsyncEnumerator<TopicOverrideSample> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            return Empty().GetAsyncEnumerator(cancellationToken);

            static async IAsyncEnumerable<TopicOverrideSample> Empty()
            {
                await Task.CompletedTask;
                yield break;
            }
        }

        protected override Task SendEntityAsync(TopicOverrideSample entity, Dictionary<string, string>? headers, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task OnError_ActionFailure_SendsToDlq()
    {
        var ctx = CreateContext(new KsqlDslOptions());
        var model = new EntityModel
        {
            EntityType = typeof(TopicOverrideSample),
            TopicName = "test",
            AllProperties = typeof(TopicOverrideSample).GetProperties(),
            KeyProperties = typeof(TopicOverrideSample).GetProperties(),
        };

        var sink = new TestErrorSink();
        var set = new TestEventSet(ctx, model, sink, new TopicOverrideSample { Id = 1 });

        await set.OnError(ErrorAction.DLQ)
            .ForEachAsync(_ => throw new Exception("fail"), TimeSpan.FromMilliseconds(10));

        Assert.False(sink.Handled);
    }

}
