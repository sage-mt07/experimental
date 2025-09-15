using Kafka.Ksql.Linq.Cache.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

public class TableCacheTests
{
    private const char NUL = '\u0000';

    private static TableCache<Dummy> MakeCache(IEnumerable<(string key, int val)> items)
    {
        // wait: 直ちにRUNNING扱い
        Task Wait(TimeSpan? _) => Task.CompletedTask;

        // 列挙: objectペアに変換
        var lazy = new Lazy<Func<IEnumerable<(object key, object val)>>>(() =>
            () => items.Select(x => ((object)x.key, (object)x.val)));

        // keyStringify: 文字列キー前提
        string KeyFmt(object k) => (string)k;

        // combiner: string key + int value -> Dummy
        object Combine(string key, object val, Type _)
        {
            var parts = key.Split(NUL);
            return new Dummy
            {
                Broker = parts.ElementAtOrDefault(0) ?? string.Empty,
                Symbol = parts.ElementAtOrDefault(1) ?? string.Empty,
                Ts = parts.ElementAtOrDefault(2) ?? string.Empty,
                V = (int)val
            };
        }

        return (TableCache<Dummy>)Activator.CreateInstance(
            typeof(TableCache<Dummy>),
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic,
            null,
            new object[] { (Func<TimeSpan?, Task>)Wait, lazy, (Func<object, string>)KeyFmt, (Func<string, object, Type, object>)Combine },
            null
        )!;
    }

    private static string K(string b, string s, string ts) => $"{b}{NUL}{s}{NUL}{ts}{NUL}";

    [Fact]
    public async Task NoFilter_ReturnsAll()
    {
        var cache = MakeCache(new[]
        {
            (K("OANDA","USDJPY","20250821T000000Z"), 1),
            (K("OANDA","EURUSD","20250821T000000Z"), 2),
        });

        var list = await cache.ToListAsync(); // filter = null
        Assert.Equal(2, list.Count);
        Assert.Contains(list, x => x.Symbol == "USDJPY" && x.V == 1);
        Assert.Contains(list, x => x.Symbol == "EURUSD" && x.V == 2);
    }

    [Fact]
    public async Task Filter_Broker_Prefix()
    {
        var cache = MakeCache(new[]
        {
            (K("OANDA","USDJPY","20250821T000000Z"), 1),
            (K("DUKA","USDJPY","20250821T000000Z"), 9),
        });

        var list = await cache.ToListAsync(new List<string> { "OANDA" });
        Assert.Single(list);
        Assert.Equal("OANDA", list[0].Broker);
    }

    [Fact]
    public async Task Filter_BrokerSymbol_Prefix()
    {
        var cache = MakeCache(new[]
        {
            (K("OANDA","USDJPY","20250821T000000Z"), 1),
            (K("OANDA","EURUSD","20250821T000000Z"), 2),
        });

        var list = await cache.ToListAsync(new List<string> { "OANDA", "USDJPY" });
        Assert.Single(list);
        Assert.Equal("USDJPY", list[0].Symbol);
    }

    [Fact]
    public async Task Filter_ThreeParts_Prefix()
    {
        var cache = MakeCache(new[]
        {
            (K("OANDA","USDJPY","20250821T000000Z"), 1),
            (K("OANDA","USDJPY","20250821T010000Z"), 2),
        });

        var list = await cache.ToListAsync(new List<string> { "OANDA", "USDJPY", "20250821T000000Z" });
        Assert.Single(list);
        Assert.Equal("20250821T000000Z", list[0].Ts);
    }

    [Fact]
    public async Task EmptyFilter_EqualsAll()
    {
        var cache = MakeCache(new[]
        {
            (K("A","B","T1"), 1),
            (K("A","C","T2"), 2),
        });

        var list = await cache.ToListAsync(new List<string>());
        Assert.Equal(2, list.Count);
    }

    [Fact]
    public async Task NoMatch_ReturnsEmpty()
    {
        var cache = MakeCache(new[]
        {
            (K("OANDA","USDJPY","T"), 1),
        });

        var list = await cache.ToListAsync(new List<string> { "DUKA" });
        Assert.Empty(list);
    }

    public class Dummy
    {
        public string Broker { get; set; } = "";
        public string Symbol { get; set; } = "";
        public string Ts { get; set; } = "";
        public int V { get; set; }
    }
}
