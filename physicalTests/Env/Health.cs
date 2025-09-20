using Confluent.Kafka;
using System;
using System.Net.Http;
using System.Threading.Tasks;

namespace PhysicalTestEnv;

public static class Health
{
    public static async Task WaitForKafkaAsync(string bootstrapServers, TimeSpan? timeout = null)
    {
        var end = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(60));
        while (DateTime.UtcNow < end)
        {
            try
            {
                using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build();
                _ = admin.GetMetadata(TimeSpan.FromSeconds(3));
                return;
            }
            catch { await Task.Delay(1000); }
        }
        throw new TimeoutException($"Kafka not reachable at {bootstrapServers}");
    }

    public static async Task WaitForHttpOkAsync(string url, TimeSpan? timeout = null)
    {
        using var http = new HttpClient();
        var end = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(60));
        while (DateTime.UtcNow < end)
        {
            try
            {
                using var resp = await http.GetAsync(url);
                if ((int)resp.StatusCode >= 200 && (int)resp.StatusCode < 500)
                    return;
            }
            catch { }
            await Task.Delay(1000);
        }
        throw new TimeoutException($"HTTP not ready: {url}");
    }
}

