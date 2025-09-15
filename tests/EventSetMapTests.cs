using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Core.Modeling;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

#nullable enable

namespace Kafka.Ksql.Linq.Tests;

public class EventSetMapTests
{
    private class DummyContext : IKsqlContext
    {
        public IEntitySet<T> Set<T>() where T : class => throw new NotImplementedException();
        public object GetEventSet(Type entityType) => throw new NotImplementedException();
        public Dictionary<Type, EntityModel> GetEntityModels() => new();
        public void Dispose() { }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    [KsqlTopic("sample-topic")]
    [KsqlTable]
    private class Sample
    {
        [KsqlKey(Order = 0)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private class SampleDto
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private class SampleSet : EventSet<Sample>
    {
        private readonly List<Sample> _items;

        public SampleSet(List<Sample> items, EntityModel model) : base(new DummyContext(), model)
        {
            _items = items;
        }

        protected override Task SendEntityAsync(Sample entity, Dictionary<string, string>? headers, CancellationToken cancellationToken) => Task.CompletedTask;

        public override async IAsyncEnumerator<Sample> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            foreach (var item in _items)
            {
                if (cancellationToken.IsCancellationRequested)
                    yield break;

                yield return item;
                await Task.Yield();
            }
        }
    }

    private static EntityModel CreateModel()
    {
        var builder = new ModelBuilder();
        builder.Entity<Sample>();
        return builder.GetEntityModel<Sample>()!;
    }

    [Fact(Skip = "Requires KsqlContext")]
    public async Task Map_ForEachAsync_ReturnsMappedValues()
    {
        var items = new List<Sample> { new Sample { Id = 1, Name = "A" } };
        var set = new SampleSet(items, CreateModel());

        var mapped = set.Map(x => new SampleDto { Id = x.Id, Name = x.Name });

        var results = new List<SampleDto>();
        await mapped.ForEachAsync((dto, _, _) =>
        {
            results.Add(dto);
            return Task.CompletedTask;
        });

        var resultDto = Assert.Single(results);
        Assert.Equal(1, resultDto.Id);
        Assert.Equal("A", resultDto.Name);
    }
}
