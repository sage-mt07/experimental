using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Threading.Tasks;

namespace Kafka.Ksql.Linq.Tests;

public static class KafkaRestProxyClient
{
    private static readonly string BaseUrl = "http://localhost:8082";

    public static async Task<string[]> FetchMessagesAsync(string topic, int partition = 0, int count = 10)
    {
        using var http = new HttpClient();
        var url = $"{BaseUrl}/topics/{Uri.EscapeDataString(topic)}/partitions/{partition}/messages?count={count}";
        var request = new HttpRequestMessage(HttpMethod.Get, url);
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/vnd.kafka.json.v2+json"));

        HttpResponseMessage response;
        try
        {
            response = await http.SendAsync(request).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            throw new KafkaRestProxyException("Failed to fetch messages from Kafka REST Proxy.", ex);
        }

        response.EnsureSuccessStatusCode();

        var content = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
        var messages = new List<string>();

        using var document = JsonDocument.Parse(content);
        foreach (var element in document.RootElement.EnumerateArray())
        {
            if (element.TryGetProperty("value", out var value))
            {
                messages.Add(value.GetRawText());
            }
        }

        return messages.ToArray();
    }
}

public class KafkaRestProxyException : Exception
{
    public KafkaRestProxyException(string message) : base(message) { }
    public KafkaRestProxyException(string message, Exception innerException) : base(message, innerException) { }
}
