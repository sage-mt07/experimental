using Kafka.Ksql.Linq;

namespace PhysicalTestEnv;

#nullable enable

public static class KsqlHelpers
{
    public static async Task<KsqlDbResponse> ExecuteStatementWithRetryAsync(KsqlContext ctx, string statement, int retries = 3, int delayMs = 1000)
    {
        Exception? last = null;
        for (var i = 0; i < retries; i++)
        {
            try
            {
                var r = await ctx.ExecuteStatementAsync(statement);
                if (r.IsSuccess) return r;
                last = new InvalidOperationException(r.Message);
            }
            catch (Exception ex)
            {
                last = ex;
            }
            await Task.Delay(delayMs);
        }
        throw last ?? new InvalidOperationException("ExecuteStatementWithRetryAsync failed without exception");
    }

    /// <summary>
    /// Wait until ksqlDB /info endpoint responds, then optionally wait a grace period.
    /// </summary>
    public static async Task WaitForKsqlReadyAsync(string ksqlBaseUrl, TimeSpan? timeout = null, int graceMs = 0)
    {
        var end = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(60));
        using var http = new HttpClient();
        var url = ksqlBaseUrl.TrimEnd('/') + "/info";
        while (DateTime.UtcNow < end)
        {
            try
            {
                using var resp = await http.GetAsync(url);
                if ((int)resp.StatusCode >= 200 && (int)resp.StatusCode < 300)
                {
                    if (graceMs > 0) await Task.Delay(graceMs);
                    return;
                }
            }
            catch { }
            await Task.Delay(1000);
        }
        throw new TimeoutException($"ksqlDB not ready: {url}");
    }

    /// <summary>
    /// Create KsqlContext (or derived) with retries. The factory is invoked per-attempt.
    /// </summary>
    public static async Task<T> CreateContextWithRetryAsync<T>(Func<T> factory, int retries = 3, int delayMs = 1000) where T : KsqlContext
    {
        Exception? last = null;
        for (var i = 0; i < retries; i++)
        {
            try
            {
                return factory();
            }
            catch (Exception ex)
            {
                last = ex;
            }
            await Task.Delay(delayMs);
        }
        throw last ?? new InvalidOperationException("CreateContextWithRetryAsync failed without exception");
    }

    public static async Task TerminateAllAsync(string ksqlBaseUrl)
    {
        using var http = new HttpClient { BaseAddress = new Uri(ksqlBaseUrl.TrimEnd('/')) };
        var payload = new { ksql = "TERMINATE ALL;", streamsProperties = new { } };
        var json = System.Text.Json.JsonSerializer.Serialize(payload);
        using var content = new StringContent(json, System.Text.Encoding.UTF8, "application/json");
        try { using var _ = await http.PostAsync("/ksql", content); } catch { }
    }

    public static async Task DropArtifactsAsync(string ksqlBaseUrl, IEnumerable<string> objectsInDependencyOrder)
    {
        using var http = new HttpClient { BaseAddress = new Uri(ksqlBaseUrl.TrimEnd('/')) };
        foreach (var obj in objectsInDependencyOrder)
        {
            var stmt = $"DROP TABLE IF EXISTS {obj} DELETE TOPIC;";
            var streamStmt = $"DROP STREAM IF EXISTS {obj} DELETE TOPIC;";
            var payload = new { ksql = stmt, streamsProperties = new { } };
            var json = System.Text.Json.JsonSerializer.Serialize(payload);
            using var content = new StringContent(json, System.Text.Encoding.UTF8, "application/json");
            try { using var resp = await http.PostAsync("/ksql", content); }
            catch { }
            // Try as stream too
            payload = new { ksql = streamStmt, streamsProperties = new { } };
            json = System.Text.Json.JsonSerializer.Serialize(payload);
            using var content2 = new StringContent(json, System.Text.Encoding.UTF8, "application/json");
            try { using var resp2 = await http.PostAsync("/ksql", content2); }
            catch { }
        }
    }

    public static Task TerminateAndDropBarArtifactsAsync(string ksqlBaseUrl)
    {
        // Drop dependents first (tables), then base stream
        var order = new[] { "bar_1m_live", "bar_5m_live", "bar_hb_1s", "bar_1s_final", "bar_1s_final_s" };
        return Task.Run(async () =>
        {
            await TerminateAllAsync(ksqlBaseUrl);
            await DropArtifactsAsync(ksqlBaseUrl, order);
        });
    }
}
