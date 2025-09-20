using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Core.Configuration;
using Kafka.Ksql.Linq.Core.Modeling;
using Kafka.Ksql.Linq.Query.Abstractions;
using Kafka.Ksql.Linq.Query.Ddl;
using Kafka.Ksql.Linq.Query.Pipeline;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Kafka.Ksql.Linq.Tests.Integration;


public class DynamicKsqlGenerationTests
{

    private static void ConfigureModel(IModelBuilder builder)
    {
        builder.Entity<OrderValue>();
        builder.Entity<Customer>();
        builder.Entity<EventLog>();
        builder.Entity<NullableOrder>();
        builder.Entity<NullableKeyOrder>();
    }

    private static Dictionary<Type, EntityModel> BuildModels()
    {
        var mb = new ModelBuilder();
        using (ModelCreatingScope.Enter())
        {
            ConfigureModel(mb);
        }
        mb.ValidateAllModels();
        return mb.GetAllEntityModels();
    }

    private static T ExecuteInScope<T>(Func<T> func)
    {
        using (ModelCreatingScope.Enter())
        {
            return func();
        }
    }

    private static IEnumerable<string> GenerateDdlQueries(Dictionary<Type, EntityModel> models)
    {
        var ddl = new DDLQueryGenerator();
        foreach (var model in models.Values)
        {
            var name = model.TopicName ?? model.EntityType.Name.ToLowerInvariant();
            var provider = new EntityModelDdlAdapter(model);
            if (model.StreamTableType == StreamTableType.Table)
                yield return ExecuteInScope(() => ddl.GenerateCreateTable(provider));
            else
                yield return ExecuteInScope(() => ddl.GenerateCreateStream(provider));
        }

        IQueryable<OrderValue> orders = new List<OrderValue>().AsQueryable();
        var tableExpr = orders
            .Where(o => o.Amount > 100)
            .GroupBy(o => o.Region)
            .Select(g => new { g.Key, Total = g.Sum(x => (double)x.Amount) });
        yield return ExecuteInScope(() => ddl.GenerateCreateTableAs("orders_by_region", "orders", tableExpr.Expression));

        IQueryable<Customer> customers = new List<Customer>().AsQueryable();
        var streamExpr = orders
            .Join(customers, o => o.CustomerId, c => c.Id, (o, c) => new { o, c })
            .Select(x => new { x.o.CustomerId, x.c.Name, x.o.Amount });
        yield return ExecuteInScope(() => ddl.GenerateCreateStreamAs("order_enriched", "orders", streamExpr.Expression));
    }

    //private static IEnumerable<(string Description, string Ksql)> GenerateDmlQueries()
    //{
    //    //using var ctx = new DummyContext(new KsqlDslOptions());
    //    //using (ModelCreatingScope.Enter())
    //    //{
    //    //    yield return ("SelectAll_Orders", ctx.Entity<OrderValue>().ToQueryString());
    //    //    yield return ("SelectAll_Customers", ctx.Entity<Customer>().ToQueryString());
    //    //    yield return ("SelectAll_Events", ctx.Entity<EventLog>().ToQueryString());
    //    //    yield return ("SelectAll_NullableOrder", ctx.Entity<NullableOrder>().ToQueryString());
    //    //    yield return ("SelectAll_NullableKeyOrder", ctx.Entity<NullableKeyOrder>().ToQueryString());

    //    //yield return ("Aggregate_Sum", ctx.Entity<OrderValue>()
    //    //    .GroupBy(o => o.CustomerId)
    //    //    .Select(g => new { g.Key, Sum = g.Sum(x => (double)x.Amount) })
    //    //    .ToQueryString());

    //    //yield return ("Aggregate_Latest", ctx.Entity<OrderValue>()
    //    //    .GroupBy(o => o.CustomerId)
    //    //    .Select(g => new { g.Key, Last = g.LatestByOffset(x => x.Id) })
    //    //    .ToQueryString());

    //    //yield return ("Aggregate_First", ctx.Entity<OrderValue>()
    //    //    .GroupBy(o => o.CustomerId)
    //    //    .Select(g => new { g.Key, First = g.EarliestByOffset(x => x.Id) })
    //    //    .ToQueryString());

    //    //yield return ("Complex_Window", ctx.Entity<OrderValue>()
    //    //    .Where(o => o.Amount > 100)
    //    //    .Window(TumblingWindow.OfMinutes(5))
    //    //    .GroupBy(o => o.CustomerId)
    //    //    .Having(g => g.Count() > 1)
    //    //    .Select(g => new { g.Key, Count = g.Count() })
    //    //    .ToQueryString());

    //    //yield return ("Join_Having", ctx.Entity<OrderValue>()
    //    //    .Join(ctx.Entity<Customer>(), o => o.CustomerId, c => c.Id, (o, c) => new { o, c })
    //    //    .GroupBy(x => x.o.CustomerId)
    //    //    .Having(g => g.Sum(x => (double)x.o.Amount) > 1000)
    //    //    .Select(g => new { g.Key, Total = g.Sum(x => (double)x.o.Amount) })
    //    //    .ToQueryString());

    //    //yield return ("GroupBy_MultiKey", ctx.Entity<OrderValue>()
    //    //    .GroupBy(o => new { o.CustomerId, o.Region })
    //    //    .Having(g => g.Sum(x => (double)x.Amount) > 500)
    //    //    .Select(g => new { g.Key.CustomerId, g.Key.Region, Total = g.Sum(x => (double)x.Amount) })
    //    //    .ToQueryString());

    //    //yield return ("Conditional_Sum", ctx.Entity<OrderValue>()
    //    //    .GroupBy(o => o.CustomerId)
    //    //    .Select(g => new
    //    //    {
    //    //        g.Key,
    //    //        Total = g.Sum(o => (double)o.Amount),
    //    //        HighPriorityTotal = g.Sum(o => o.IsHighPriority ? (double)o.Amount : 0d)
    //    //    })
    //    //    .ToQueryString());

    //    //yield return ("Aggregate_AvgMinMax", ctx.Entity<OrderValue>()
    //    //    .GroupBy(o => o.CustomerId)
    //    //    .Select(g => new
    //    //    {
    //    //        g.Key,
    //    //        AverageAmount = g.Average(o => (double)o.Amount),
    //    //        MinAmount = g.Min(o => o.Amount),
    //    //        MaxAmount = g.Max(o => o.Amount)
    //    //    })
    //    //    .ToQueryString());

    //    //yield return ("OrderByDesc", ctx.Entity<OrderValue>()
    //    //    .GroupBy(o => o.CustomerId)
    //    //    .Select(g => new { g.Key, Total = g.Sum(o => (double)o.Amount) })
    //    //    .ToQueryString());

    //    //yield return ("OrderByThenBy", ctx.Entity<OrderValue>()
    //    //    .GroupBy(o => new { o.CustomerId, o.Region })
    //    //    .Select(g => new { g.Key.CustomerId, g.Key.Region, Total = g.Sum(o => (double)o.Amount) })
    //    //    .ToQueryString());

    //    //yield return ("Complex_Having", ctx.Entity<OrderValue>()
    //    //    .GroupBy(o => new { o.CustomerId, o.Region })
    //    //    .Having(g => (g.Sum(x => (double)x.Amount) > 1000 && g.Count() > 10) || g.Average(x => (double)x.Amount) > 150)
    //    //    .Select(g => new
    //    //    {
    //    //        g.Key.CustomerId,
    //    //        g.Key.Region,
    //    //        TotalAmount = g.Sum(x => (double)x.Amount),
    //    //        OrderCount = g.Count(),
    //    //        AverageAmount = g.Average(x => (double)x.Amount)
    //    //    })
    //    //    .ToQueryString());

    //    //yield return ("Case_When", ctx.Entity<OrderValue>()
    //    //    .GroupBy(o => o.CustomerId)
    //    //    .Select(g => new
    //    //    {
    //    //        g.Key,
    //    //        Total = g.Sum(o => (double)o.Amount),
    //    //        Status = g.Sum(o => (double)o.Amount) > 1000 ? "VIP" : "Regular"
    //    //    })
    //    //    .ToQueryString());

    //    //yield return ("GroupWhereHaving", ctx.Entity<OrderValue>()
    //    //    .GroupBy(o => o.CustomerId)
    //    //    .Where(g => (g.Sum(o => (double)o.Amount) > 1000 && g.Count() > 5) || g.Average(o => (double)o.Amount) > 500)
    //    //    .Select(g => new
    //    //    {
    //    //        g.Key,
    //    //        Total = g.Sum(o => (double)o.Amount),
    //    //        Count = g.Count(),
    //    //        Avg = g.Average(o => (double)o.Amount)
    //    //    })
    //    //    .ToQueryString());

    //    //yield return ("Or_Having", ctx.Entity<OrderValue>()
    //    //    .GroupBy(o => o.CustomerId)
    //    //    .Where(g => g.Sum(x => (double)x.Amount) > 1000 || g.Sum(x => x.Count) > 5)
    //    //    .Select(g => new { g.Key, TotalAmount = g.Sum(x => (double)x.Amount), TotalCount = g.Sum(x => x.Count) })
    //    //    .ToQueryString());

    //    //var excluded = new[] { "CN", "RU" };
    //    //yield return ("Not_In", ctx.Entity<OrderValue>()
    //    //    .Where(o => !excluded.Contains(o.Region))
    //    //    .Select(o => new { o.CustomerId, o.Region, o.Amount })
    //    //    .ToQueryString());

    //    //yield return ("IsNull", ctx.Entity<NullableOrder>()
    //    //    .Where(o => o.CustomerId == null)
    //    //    .Select(o => new { o.Region, o.Amount })
    //    //    .ToQueryString());

    //    //yield return ("IsNotNull", ctx.Entity<NullableOrder>()
    //    //    .Where(o => o.CustomerId != null)
    //    //    .Select(o => new { o.Region, o.Amount })
    //    //    .ToQueryString());

    //    //yield return ("Group_NullableKey", ctx.Entity<NullableKeyOrder>()
    //    //    .Where(o => o.CustomerId != null)
    //    //    .GroupBy(o => o.CustomerId)
    //    //    .Select(g => new { CustomerId = g.Key, Total = g.Sum(x => (double)x.Amount) })
    //    //    .ToQueryString());

    //    //yield return ("Expr_Key", ctx.Entity<OrderValue>()
    //    //    .GroupBy(o => o.Region.ToUpper())
    //    //    .Having(g => g.Sum(x => (double)x.Amount) > 500)
    //    //    .Select(g => new { RegionUpper = g.Key, TotalAmount = g.Sum(x => (double)x.Amount) })
    //    //    .ToQueryString());
    //}
    //}

    // OnModelCreating で生成したモデルから DDL/DML が正しく実行できるか検証
    //[Fact]
    //[Trait("Category", "Integration")]
    //public async Task CreateAllObjectsByOnModelCreating()
    //{


    //    await EnvDynamicKsqlGenerationTests.ResetAsync();

    //    var models = BuildModels();
    //    var ddls = GenerateDdlQueries(models).ToList();

    //    foreach (var ddl in ddls)
    //    {
    //        var drop = ddl.StartsWith("CREATE STREAM", StringComparison.OrdinalIgnoreCase)
    //            ? ddl.Replace("CREATE STREAM", "DROP STREAM IF EXISTS", StringComparison.OrdinalIgnoreCase) + " DELETE TOPIC;"
    //            : ddl.Replace("CREATE TABLE", "DROP TABLE IF EXISTS", StringComparison.OrdinalIgnoreCase) + " DELETE TOPIC;";
    //        await ExecuteStatementDirectAsync(drop);
    //    }

    //    foreach (var ddl in ddls)
    //    {
    //        var result = await ExecuteStatementDirectAsync(ddl);
    //        var success = result.IsSuccess ||
    //            (result.Message?.Contains("already exists", StringComparison.OrdinalIgnoreCase) ?? false);
    //        Assert.True(success, $"DDL failed: {result.Message}");
    //    }

    //    var timeout = TimeSpan.FromSeconds(5);
    //    foreach (var model in models.Values)
    //    {
    //        var isTable = model.StreamTableType == StreamTableType.Table;
    //        var name = (model.TopicName ?? model.EntityType.Name).ToUpperInvariant();
    //        await WaitForEntityReadyDirectAsync(name, isTable, timeout);
    //    }

    //    var tables = await ExecuteStatementDirectAsync("SHOW TABLES;");
    //    Assert.Contains("ORDERS", tables.Message, StringComparison.OrdinalIgnoreCase);
    //    Assert.Contains("CUSTOMERS", tables.Message, StringComparison.OrdinalIgnoreCase);
    //    Assert.Contains("EVENTS", tables.Message, StringComparison.OrdinalIgnoreCase);

    //    var streams = await ExecuteStatementDirectAsync("SHOW STREAMS;");
    //    Assert.Contains("ORDERS_NULLABLE", streams.Message, StringComparison.OrdinalIgnoreCase);
    //    Assert.Contains("ORDERS_NULLABLE_KEY", streams.Message, StringComparison.OrdinalIgnoreCase);

    //    var describe = await ExecuteStatementDirectAsync("DESCRIBE ORDERS;");
    //    Assert.Contains("CUSTOMERID", describe.Message.ToUpperInvariant());
    //    Assert.Contains("AMOUNT", describe.Message.ToUpperInvariant());

    //    foreach (var (_, ksql) in GenerateDmlQueries())
    //    {
    //        var response = await ExecuteExplainDirectAsync(ksql);
    //        Assert.True(response.IsSuccess, $"{ksql} failed: {response.Message}");
    //    }
    //}

    //public static IEnumerable<object[]> AllDmlQueries()
    //{
    //    var queries = GenerateDmlQueries().ToList();
    //    if (queries.Count == 0)
    //        yield return new object[] { "SELECT 1;" };
    //    else
    //        foreach (var q in queries)
    //            yield return new object[] { q.Ksql };
    //}

    // 生成したすべてのDMLクエリがksqlDBで有効か確認
    //[Theory]
    //[Trait("Category", "Integration")]
    //[MemberData(nameof(AllDmlQueries), MemberType = typeof(DynamicKsqlGenerationTests))]
    //public async Task AllDmlQueries_ShouldBeValidInKsqlDb(string ksql)
    //{

    //    try
    //    {
    //        await EnvDynamicKsqlGenerationTests.ResetAsync();

    //    }
    //    catch (Exception)
    //    {
    //    }

    //    if (!TestSchema.IsSupportedKsql(ksql))
    //        return; // skip unsupported queries

    //    TestSchema.ValidateDmlQuery(ksql);
    //    await using var ctx = EnvDynamicKsqlGenerationTests.CreateContext();
    //    var response = await ctx.ExecuteExplainAsync(ksql);
    //    Assert.True(response.IsSuccess, $"{ksql} failed: {response.Message}");
    //}

    // sample entities
    [KsqlTopic("orders")]
    public class OrderValue
    {
        public int CustomerId { get; set; }
        public int Id { get; set; }
        public string Region { get; set; } = string.Empty;
        public double Amount { get; set; }
        public bool IsHighPriority { get; set; }
        public int Count { get; set; }
    }

    [KsqlTopic("customers")]
    public class Customer
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [KsqlTopic("events")]
    public class EventLog
    {
        public int Level { get; set; }
        public string Message { get; set; } = string.Empty;
    }

    [KsqlTopic("orders_nullable")]
    public class NullableOrder
    {
        public int? CustomerId { get; set; }
        public string Region { get; set; } = string.Empty;
        public double Amount { get; set; }
    }

    [KsqlTopic("orders_nullable_key")]
    public class NullableKeyOrder
    {
        public int? CustomerId { get; set; }
        public double Amount { get; set; }
    }

    public class DummyContext : KsqlContext
    {
        public DummyContext() : base(new KsqlDslOptions()) { }
        public DummyContext(KsqlDslOptions options) : base(options) { }
        protected override void OnModelCreating(IModelBuilder modelBuilder)
        {
            DynamicKsqlGenerationTests.ConfigureModel(modelBuilder);
        }
    }

    private async Task ProduceDummyRecordsAsync()
    {
        var options = new KsqlDslOptions
        {
            Common = new CommonSection { BootstrapServers = EnvDynamicKsqlGenerationTests.KafkaBootstrapServers },
            SchemaRegistry = new SchemaRegistrySection { Url = EnvDynamicKsqlGenerationTests.SchemaRegistryUrl }
        };

        await using var ctx = new DummyContext(options);

        await ctx.Set<OrderValue>().AddAsync(new OrderValue
        {
            CustomerId = 1,
            Id = 1,
            Region = "east",
            Amount = 10d,
            IsHighPriority = false,
            Count = 1
        });

        await ctx.Set<Customer>().AddAsync(new Customer { Id = 1, Name = "alice" });
        await ctx.Set<EventLog>().AddAsync(new EventLog { Level = 1, Message = "init" });
        await ctx.Set<NullableOrder>().AddAsync(new NullableOrder { CustomerId = 1, Region = "east", Amount = 10d });
        await ctx.Set<NullableKeyOrder>().AddAsync(new NullableKeyOrder { CustomerId = 1, Amount = 10d });

        var timeout = TimeSpan.FromSeconds(5);
        await ctx.WaitForEntityReadyAsync<OrderValue>(timeout);
        await ctx.WaitForEntityReadyAsync<Customer>(timeout);
        await ctx.WaitForEntityReadyAsync<EventLog>(timeout);
        await ctx.WaitForEntityReadyAsync<NullableOrder>(timeout);
        await ctx.WaitForEntityReadyAsync<NullableKeyOrder>(timeout);

        await ctx.DisposeAsync();
    }

    private static async Task<KsqlDbResponse> ExecuteStatementDirectAsync(string statement)
    {
        using var client = new HttpClient { BaseAddress = new Uri(EnvDynamicKsqlGenerationTests.KsqlDbUrl) };
        var payload = new { ksql = statement, streamsProperties = new { } };
        var json = JsonSerializer.Serialize(payload);
        using var content = new StringContent(json, Encoding.UTF8, "application/json");
        using var response = await client.PostAsync("/ksql", content);
        var body = await response.Content.ReadAsStringAsync();
        var success = response.IsSuccessStatusCode && !body.Contains("\"error_code\"");
        return new KsqlDbResponse(success, body);
    }

    private static async Task<KsqlDbResponse> ExecuteExplainDirectAsync(string ksql)
    {
        return await ExecuteStatementDirectAsync($"EXPLAIN {ksql}");
    }

    private static async Task<bool> IsEntityReadyDirectAsync(string name, bool isTable)
    {
        var statement = isTable ? "SHOW TABLES;" : "SHOW STREAMS;";
        var response = await ExecuteStatementDirectAsync(statement);
        if (!response.IsSuccess)
            return false;

        try
        {
            using var doc = JsonDocument.Parse(response.Message);
            var listName = isTable ? "tables" : "streams";
            foreach (var item in doc.RootElement.EnumerateArray())
            {
                if (!item.TryGetProperty(listName, out var arr))
                    continue;

                foreach (var element in arr.EnumerateArray())
                {
                    if (element.TryGetProperty("name", out var n) &&
                        string.Equals(n.GetString(), name, StringComparison.OrdinalIgnoreCase))
                    {
                        return true;
                    }
                }
            }
        }
        catch
        {
            // ignore parse errors
        }

        return false;
    }

    private static async Task WaitForEntityReadyDirectAsync(string name, bool isTable, TimeSpan timeout)
    {
        var start = DateTime.UtcNow;
        while (DateTime.UtcNow - start < timeout)
        {
            if (await IsEntityReadyDirectAsync(name, isTable))
                return;

            await Task.Delay(100);
        }

        throw new TimeoutException($"Entity {name} not ready after {timeout}.");
    }
}

// local environment helpers
public class EnvDynamicKsqlGenerationTests
{
    internal const string SchemaRegistryUrl = "http://127.0.0.1:18081";
    internal const string KsqlDbUrl = "http://127.0.0.1:18088";
    internal const string KafkaBootstrapServers = "127.0.0.1:39092";
    internal const string SkipReason = "Skipped in CI due to missing ksqlDB instance or schema setup failure";

    internal static bool IsKsqlDbAvailable()
    {
        try
        {
            using var ctx = CreateContext();
            var r = ctx.ExecuteStatementAsync("SHOW TOPICS;").GetAwaiter().GetResult();
            return r.IsSuccess;
        }
        catch
        {
            return false;
        }
    }

    internal static KsqlContext CreateContext()
    {
        var options = new KsqlDslOptions
        {
            Common = new CommonSection { BootstrapServers = KafkaBootstrapServers },
            SchemaRegistry = new SchemaRegistrySection { Url = SchemaRegistryUrl },
            KsqlDbUrl = KsqlDbUrl
        };
        return new BasicContext(options);
    }

    internal static Task ResetAsync() => Task.CompletedTask;
    internal static Task SetupAsync() => Task.CompletedTask;

    private class BasicContext : KsqlContext
    {
        public BasicContext(KsqlDslOptions options) : base(options) { }
        protected override bool SkipSchemaRegistration => true;
        protected override IEntitySet<T> CreateEntitySet<T>(EntityModel entityModel) => throw new NotImplementedException();
        protected override void OnModelCreating(IModelBuilder modelBuilder) { }
    }
}
