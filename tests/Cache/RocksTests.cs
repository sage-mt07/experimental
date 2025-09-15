using Kafka.Ksql.Linq.Cache.Core;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Query.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Cache;

public class RocksTests
{
    private const char N = '\u0000';
    private static TableCache<Dummy> MakeCache(IEnumerable<(string key, int val)> items)
    {
        Task Wait(TimeSpan? _) => Task.CompletedTask;
        var lazy = new Lazy<Func<IEnumerable<(object key, object val)>>>(() =>
            () => items.Select(x => ((object)x.key, (object)x.val)));
        string KeyFmt(object k) => (string)k;
        object Combine(string key, object val, Type _) => new Dummy { K = key, V = (int)val };
        return (TableCache<Dummy>)Activator.CreateInstance(
            typeof(TableCache<Dummy>),
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic,
            null,
            new object[] { (Func<TimeSpan?, Task>)Wait, lazy, (Func<object, string>)KeyFmt, (Func<string, object, Type, object>)Combine },
            null
        )!;
    }

    [Fact]
    public void Rocks_Only_For_Table()
    {
        var table = new EntityModel();
        var stream = new EntityModel { EnableCache = false };
        stream.SetStreamTableType(StreamTableType.Stream);
        Assert.True(table.EnableCache);
        Assert.False(stream.EnableCache);
    }

    [Fact]
    public async Task Rocks_Final_Queryable_After_Restart()
    {
        var items = new[] { ($"a{N}b{N}t{N}", 1) };
        var cache1 = MakeCache(items);
        await cache1.ToListAsync();
        var cache2 = MakeCache(items);
        var list = await cache2.ToListAsync();
        Assert.Single(list);
        Assert.Equal(1, list[0].V);
    }

    public class Dummy
    {
        public string K { get; set; } = string.Empty;
        public int V { get; set; }
    }
}
