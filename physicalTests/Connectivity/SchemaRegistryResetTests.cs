using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Configuration;
using System;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Json;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Integration;

#nullable enable

[Collection("Schema")]
public class SchemaRegistryResetTests
{
    private static readonly HttpClient Http = new();

    private static bool IsKsqlDbAvailable()
    {
        lock (_sync)
        {
            if (_available)
                return true;

            if (_lastFailure.HasValue && DateTime.UtcNow - _lastFailure.Value < TimeSpan.FromSeconds(5))
                return false;

            const int attempts = 3;
            for (var i = 0; i < attempts; i++)
            {
                try
                {
                    using var ctx = EnvSchemaRegistryResetTests.CreateContext();
                    var r = ctx.ExecuteStatementAsync("SHOW TOPICS;").GetAwaiter().GetResult();
                    if (r.IsSuccess)
                    {
                        _available = true;
                        return true;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"ksqlDB check attempt {i + 1} failed: {ex.Message}");
                }

                Thread.Sleep(1000);
            }

            _available = false;
            _lastFailure = DateTime.UtcNow;
            return false;
        }
    }

    private static bool _available;
    private static DateTime? _lastFailure;
    private static readonly object _sync = new();

    // Reset 後に全テーブルのスキーマが登録されているか確認
    [Fact]
    [Trait("Category", "Integration")]
    public async Task Setup_ShouldRegisterAllSchemas()
    {
        // if (!IsKsqlDbAvailable())
        //     throw new SkipException("ksqlDB unavailable");

        await EnvSchemaRegistryResetTests.ResetAsync();
        await EnvSchemaRegistryResetTests.SetupAsync();

        var subjects = await Http.GetFromJsonAsync<string[]>($"{EnvSchemaRegistryResetTests.SchemaRegistryUrl}/subjects");
        Assert.NotNull(subjects);

        foreach (var table in TestSchema.AllTopicNames)
        {
            Assert.Contains($"{table}-value", subjects);
            Assert.Contains($"{table}-key", subjects);
        }
        Assert.Contains("source-value", subjects);
    }

    // 既存スキーマを再登録しても成功するか確認
    [Fact]
    [Trait("Category", "Integration")]
    public async Task DuplicateSchemaRegistration_ShouldSucceed()
    {
        // if (!IsKsqlDbAvailable())
        //     throw new SkipException("ksqlDB unavailable");

        await EnvSchemaRegistryResetTests.ResetAsync();
        await EnvSchemaRegistryResetTests.SetupAsync();

        JsonElement latest;
        // Retry fetching latest schema
        for (var i = 0; ; i++)
        {
            try
            {
                latest = await Http.GetFromJsonAsync<JsonElement>($"{EnvSchemaRegistryResetTests.SchemaRegistryUrl}/subjects/orders-value/versions/latest");
                break;
            }
            catch when (i < 4)
            {
                await Task.Delay(500);
            }
        }
        var schema = latest.GetProperty("schema").GetString();
        // Retry re-registering same schema
        for (var i = 0; ; i++)
        {
            try
            {
                var resp = await Http.PostAsJsonAsync($"{EnvSchemaRegistryResetTests.SchemaRegistryUrl}/subjects/orders-value/versions", new { schema });
                resp.EnsureSuccessStatusCode();
                break;
            }
            catch when (i < 4)
            {
                await Task.Delay(500);
            }
        }
    }

    // 大文字のサブジェクト名が存在しないことを確認
    [Fact]
    [Trait("Category", "Integration")]
    public async Task UpperCaseSubjects_ShouldNotExist()
    {
        // if (!IsKsqlDbAvailable())
        //     throw new SkipException("ksqlDB unavailable");
        await EnvSchemaRegistryResetTests.ResetAsync();
        await EnvSchemaRegistryResetTests.SetupAsync();
        var subjects = await Http.GetFromJsonAsync<string[]>($"{EnvSchemaRegistryResetTests.SchemaRegistryUrl}/subjects");
        Assert.NotNull(subjects);

        foreach (var table in TestSchema.AllTopicNames)
        {
            Assert.DoesNotContain($"{table.ToUpperInvariant()}-value", subjects);
            Assert.DoesNotContain($"{table.ToUpperInvariant()}-key", subjects);
        }
    }
}

// local environment helpers
public class EnvSchemaRegistryResetTests
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

    internal static async Task ResetAsync()
    {
        using var client = new HttpClient();
        string[]? subjects = null;
        try { subjects = await client.GetFromJsonAsync<string[]>($"{SchemaRegistryUrl}/subjects"); }
        catch { /* ignore */ }
        if (subjects == null) return;
        foreach (var s in subjects)
        {
            try
            {
                var url = $"{SchemaRegistryUrl}/subjects/{Uri.EscapeDataString(s)}?permanent=true";
                await client.DeleteAsync(url);
            }
            catch { /* ignore */ }
        }
    }

    internal static async Task SetupAsync()
    {
        await PhysicalTestEnv.Health.WaitForKafkaAsync(KafkaBootstrapServers, TimeSpan.FromSeconds(120));
        await PhysicalTestEnv.Health.WaitForHttpOkAsync($"{SchemaRegistryUrl}/subjects", TimeSpan.FromSeconds(120));
        await PhysicalTestEnv.Health.WaitForHttpOkAsync($"{KsqlDbUrl}/info", TimeSpan.FromSeconds(120));
        using var client = new HttpClient();
        var subjects = TestSchema.AllTopicNames
            .SelectMany(t => new[] { $"{t}-value", $"{t}-key" })
            .Concat(new[] { "source-value" })
            .Distinct()
            .ToArray();

        var schemaObject = new
        {
            type = "record",
            name = "DummyRecord",
            fields = new object[] { new { name = "x", type = "string" } }
        };
        var schemaJson = JsonSerializer.Serialize(schemaObject);
        foreach (var subject in subjects)
        {
            var payload = new { schema = schemaJson };
            var ok = false; Exception? last = null;
            for (var i = 0; i < 3 && !ok; i++)
            {
                try
                {
                    var resp = await client.PostAsJsonAsync($"{SchemaRegistryUrl}/subjects/{subject}/versions", payload);
                    resp.EnsureSuccessStatusCode();
                    ok = true;
                }
                catch (Exception ex)
                {
                    last = ex; await Task.Delay(1000);
                }
            }
            if (!ok && last != null) throw last;
        }
    }

    private class BasicContext : KsqlContext
    {
        public BasicContext(KsqlDslOptions options) : base(options) { }
        protected override bool SkipSchemaRegistration => true;
        protected override IEntitySet<T> CreateEntitySet<T>(EntityModel entityModel) => throw new NotImplementedException();
        protected override void OnModelCreating(IModelBuilder modelBuilder) { }
    }
}
