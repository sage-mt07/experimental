using Kafka.Ksql.Linq.Query.Builders.Functions;
using System.Linq;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Builders.Functions;

public class KsqlFunctionRegistryTests
{
    [Theory]
    [InlineData("Sum", "DOUBLE")]
    [InlineData("Count", "BIGINT")]
    [InlineData("Max", "ANY")]
    [InlineData("Min", "ANY")]
    [InlineData("TopK", "ARRAY")]
    [InlineData("Histogram", "MAP")]
    [InlineData("FooBar", "UNKNOWN")]
    [InlineData("sUm", "DOUBLE")]
    public void InferTypeFromMethodName_ReturnsExpected(string methodName, string expected)
    {
        var result = KsqlFunctionRegistry.InferTypeFromMethodName(methodName);
        Assert.Equal(expected, result);
    }

    [Fact]
    public void GetSpecialHandlingFunctions_IncludesParseAndConvert()
    {
        var result = KsqlFunctionRegistry.GetSpecialHandlingFunctions();
        Assert.Contains("Parse", result);
        Assert.Contains("Convert", result);
    }

    [Fact]
    public void GetSpecialHandlingFunctions_OnlyReturnsRequiresSpecial()
    {
        var expected = KsqlFunctionRegistry.GetAllMappings()
            .Where(m => m.Value.RequiresSpecialHandling)
            .Select(m => m.Key)
            .ToHashSet();

        var result = KsqlFunctionRegistry.GetSpecialHandlingFunctions();
        Assert.True(result.SetEquals(expected));
    }

    [Fact]
    public void GetFunctionsByCategory_ContainsAllCategories()
    {
        var categories = KsqlFunctionRegistry.GetFunctionsByCategory();
        var expected = new[]
        {
            "String", "Math", "Date", "Aggregate", "Array", "JSON",
            "Cast", "Conditional", "URL", "GEO", "Crypto"
        };

        foreach (var key in expected)
        {
            Assert.Contains(key, categories.Keys);
        }
    }

    [Fact]
    public void GetFunctionsByCategory_StringCategoryContents()
    {
        var categories = KsqlFunctionRegistry.GetFunctionsByCategory();
        var list = categories["String"];

        Assert.Contains("ToUpper", list);
        Assert.Contains("Trim", list);
        Assert.Contains("Contains", list);
    }

    [Fact]
    public void GetFunctionsByCategory_AggregateCategoryContents()
    {
        var categories = KsqlFunctionRegistry.GetFunctionsByCategory();
        var list = categories["Aggregate"];

        Assert.Contains("Sum", list);
        Assert.Contains("Count", list);
        Assert.Contains("Max", list);
    }

    [Fact]
    public void GetFunctionsByCategory_NoEmptyLists()
    {
        var categories = KsqlFunctionRegistry.GetFunctionsByCategory();

        foreach (var kvp in categories)
        {
            Assert.True(kvp.Value.Count > 0);
        }
    }

    private static System.Collections.Generic.Dictionary<string, KsqlFunctionMapping> GetRegistryDictionary()
    {
        var field = typeof(KsqlFunctionRegistry).GetField("_functionMappings", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        return (System.Collections.Generic.Dictionary<string, KsqlFunctionMapping>)field.GetValue(null)!;
    }

    [Fact]
    public void GetDebugInfo_IncludesKnownFunctionNames()
    {
        var info = KsqlFunctionRegistry.GetDebugInfo();

        Assert.Contains("[String]", info);
        Assert.Contains("\u2022 ToUpper \u2192 UPPER", info); // bullet "•" and arrow "→"
    }

    [Fact]
    public void GetDebugInfo_AllCategoriesPresent()
    {
        var info = KsqlFunctionRegistry.GetDebugInfo();
        var categories = new[] { "String", "Math", "Date", "Aggregate", "Array", "JSON", "Cast", "Conditional", "URL", "GEO", "Crypto" };

        foreach (var cat in categories)
        {
            Assert.Contains($"[{cat}]", info);
        }
    }

    [Fact]
    public void RegisterCustomMapping_AddsAndRetrievable()
    {
        var dict = GetRegistryDictionary();
        dict.TryGetValue("MyFunc", out var original);
        try
        {
            var mapping = new KsqlFunctionMapping("MY_FUNC", 1);
            KsqlFunctionRegistry.RegisterCustomMapping("MyFunc", mapping);

            var result = KsqlFunctionRegistry.GetMapping("MyFunc");
            Assert.Same(mapping, result);
        }
        finally
        {
            if (original is not null)
                dict["MyFunc"] = original;
            else
                dict.Remove("MyFunc");
        }
    }

    [Fact]
    public void RegisterCustomMapping_OverridesExisting()
    {
        var dict = GetRegistryDictionary();
        var original = dict["ToUpper"];
        try
        {
            var mapping = new KsqlFunctionMapping("UP", 1);
            KsqlFunctionRegistry.RegisterCustomMapping("ToUpper", mapping);

            var result = KsqlFunctionRegistry.GetMapping("ToUpper");
            Assert.Same(mapping, result);
            Assert.Equal("UP", result?.KsqlFunction);
        }
        finally
        {
            dict["ToUpper"] = original;
        }
    }

    [Fact]
    public void GetDebugInfo_ReflectsCustomMapping()
    {
        var dict = GetRegistryDictionary();
        dict.TryGetValue("MyFunc", out var original);
        try
        {
            var mapping = new KsqlFunctionMapping("MY_FUNC", 1);
            KsqlFunctionRegistry.RegisterCustomMapping("MyFunc", mapping);

            var info = KsqlFunctionRegistry.GetDebugInfo();
            Assert.Contains("MyFunc", info);
            Assert.Contains("MY_FUNC", info);
        }
        finally
        {
            if (original is not null)
                dict["MyFunc"] = original;
            else
                dict.Remove("MyFunc");
        }
    }
}
