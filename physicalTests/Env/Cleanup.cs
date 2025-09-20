using Confluent.Kafka;
using Confluent.Kafka.Admin;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace PhysicalTestEnv;

public static class Cleanup
{
    public static async Task DeleteTopicsAsync(string bootstrapServers, IEnumerable<string> topics, TimeSpan? timeout = null)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build();
        try
        {
            var names = topics.Distinct(StringComparer.OrdinalIgnoreCase).ToList();
            await admin.DeleteTopicsAsync(names, new DeleteTopicsOptions { OperationTimeout = timeout ?? TimeSpan.FromSeconds(15) });
        }
        catch (DeleteTopicsException)
        {
            // Best-effort: ignore missing topics or transient failures
        }
        catch (KafkaException)
        {
            // Broker not ready; ignore to avoid blocking tests
        }
    }

    public static async Task DeleteSubjectsAsync(string schemaRegistryUrl, IEnumerable<string> topics, CancellationToken ct = default)
    {
        using var http = new HttpClient();
        var baseUrl = schemaRegistryUrl.TrimEnd('/');
        var wanted = new System.Collections.Generic.HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var t in topics.Distinct(StringComparer.OrdinalIgnoreCase))
        {
            wanted.Add($"{t}-key");
            wanted.Add($"{t}-value");
        }

        // Fetch existing subjects and intersect with targets
        string[] list = System.Array.Empty<string>();
        try
        {
            var url = $"{baseUrl}/subjects";
            using var resp = await http.GetAsync(url, ct);
            var body = await resp.Content.ReadAsStringAsync(ct);
            list = JsonSerializer.Deserialize<string[]>(body) ?? System.Array.Empty<string>();
        }
        catch { }

        foreach (var s in list)
        {
            if (!wanted.Contains(s)) continue;
            try
            {
                var url = $"{baseUrl}/subjects/{System.Uri.EscapeDataString(s)}?permanent=true";
                using var req = new HttpRequestMessage(HttpMethod.Delete, url);
                using var resp = await http.SendAsync(req, ct);
            }
            catch { }
        }

        // Poll until subjects disappear (best-effort)
        var end = System.DateTime.UtcNow + System.TimeSpan.FromSeconds(5);
        while (System.DateTime.UtcNow < end)
        {
            try
            {
                var url = $"{baseUrl}/subjects";
                using var resp = await http.GetAsync(url, ct);
                var body = await resp.Content.ReadAsStringAsync(ct);
                var current = JsonSerializer.Deserialize<string[]>(body) ?? System.Array.Empty<string>();
                if (!System.Linq.Enumerable.Any(current, x => wanted.Contains(x))) break;
            }
            catch { break; }
            await System.Threading.Tasks.Task.Delay(250, ct);
        }
    }

    public static void DeleteLocalRocksDbState()
    {
        try
        {
            var tmp = Path.GetTempPath();
            foreach (var dir in Directory.GetDirectories(tmp, "ksql-dsl-app-*"))
            {
                try { Directory.Delete(dir, true); } catch { }
            }
        }
        catch { }
    }
}
