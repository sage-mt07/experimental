using Kafka.Ksql.Linq;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;

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
    /// Wait until ksqlDB is stable: /healthcheck ready and SHOW QUERIES responds cleanly
    /// for a given number of consecutive times, then optionally wait a settle period.
    /// </summary>
    public static async Task WaitForKsqlStableAsync(string ksqlBaseUrl, int consecutiveOk = 5, TimeSpan? timeout = null, int settleMs = 0)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(120));
        using var http = new HttpClient { BaseAddress = new Uri(ksqlBaseUrl.TrimEnd('/')) };

        // Phase 1: /healthcheck consecutive OK
        var consec = 0;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                using var resp = await http.GetAsync("/healthcheck");
                if ((int)resp.StatusCode >= 200 && (int)resp.StatusCode < 300)
                {
                    consec++;
                    if (consec >= consecutiveOk) break;
                }
                else consec = 0;
            }
            catch { consec = 0; }
            await Task.Delay(2000);
        }
        if (consec < consecutiveOk)
            throw new TimeoutException("ksqlDB /healthcheck did not stabilize in time");

        // Phase 2: SHOW QUERIES returns array and no error, consecutive OK
        consec = 0;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                var payload = new { ksql = "SHOW QUERIES;", streamsProperties = new { } };
                using var content = new StringContent(System.Text.Json.JsonSerializer.Serialize(payload), System.Text.Encoding.UTF8, "application/json");
                using var resp = await http.PostAsync("/ksql", content);
                var body = await resp.Content.ReadAsStringAsync();
                if ((int)resp.StatusCode >= 200 && (int)resp.StatusCode < 300
                    && body.IndexOf("error", StringComparison.OrdinalIgnoreCase) < 0
                    && body.IndexOf("exception", StringComparison.OrdinalIgnoreCase) < 0
                    && body.TrimStart().StartsWith("["))
                {
                    consec++;
                    if (consec >= consecutiveOk) break;
                }
                else consec = 0;
            }
            catch { consec = 0; }
            await Task.Delay(2000);
        }
        if (consec < consecutiveOk)
            throw new TimeoutException("ksqlDB SHOW QUERIES did not stabilize in time");

        if (settleMs > 0)
            await Task.Delay(settleMs);
    }

    /// <summary>
    /// Backward-compatible: minimal readiness (uses /healthcheck) then optional grace period.
    /// </summary>
    public static async Task WaitForKsqlReadyAsync(string ksqlBaseUrl, TimeSpan? timeout = null, int graceMs = 0)
    {
        await WaitForKsqlStableAsync(ksqlBaseUrl, consecutiveOk: 3, timeout: timeout ?? TimeSpan.FromSeconds(90), settleMs: graceMs);
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
