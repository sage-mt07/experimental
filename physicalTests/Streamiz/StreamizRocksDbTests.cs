using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Kafka.Ksql.Linq.Tests.Integration.Streamiz.Models;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SchemaRegistry.SerDes.Avro;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Table;
using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Xunit;

#nullable enable
namespace Kafka.Ksql.Linq.Tests.Integration.Streamiz;

[Collection("Streamiz")]
public class StreamizRocksDbTests
{
    private static async Task WaitUntilRunningAsync(KafkaStream stream, TimeSpan? timeout = null)
    {
        var stateProp = typeof(KafkaStream).GetProperty("StreamState", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);
        var end = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(90));
        while ((KafkaStream.State)stateProp!.GetValue(stream)! != KafkaStream.State.RUNNING)
        {
            if (DateTime.UtcNow > end)
                throw new TimeoutException("KafkaStream failed to reach RUNNING state");
            await Task.Delay(100);
        }
    }

    private static async Task EnsureTopicAsync(string topic)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "127.0.0.1:39092" }).Build();
        try
        {
            await admin.CreateTopicsAsync(new[] { new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 } });
        }
        catch (CreateTopicsException e)
        {
            if (e.Results.Any(r => r.Error.Code != ErrorCode.TopicAlreadyExists))
                throw;
        }
    }
    private static Materialized<TKey, TValue, IKeyValueStore<Bytes, byte[]>> CreateAvroMaterialized<TKey, TValue>(string storeName)
    {
        var materializedType = typeof(Materialized<,,>).MakeGenericType(typeof(TKey), typeof(TValue), typeof(IKeyValueStore<Bytes, byte[]>));
        var createMethod = materializedType.GetMethods(BindingFlags.Public | BindingFlags.Static)
            .First(m => m.Name == "Create" && m.IsGenericMethodDefinition && m.GetParameters().Length == 1)
            .MakeGenericMethod(typeof(SchemaAvroSerDes<>).MakeGenericType(typeof(TKey)),
                               typeof(SchemaAvroSerDes<>).MakeGenericType(typeof(TValue)));
        return (Materialized<TKey, TValue, IKeyValueStore<Bytes, byte[]>>)createMethod.Invoke(null, new object[] { storeName })!;
    }

    private static async Task StartWithRetryAsync(KafkaStream stream, int retries = 5)
    {
        var loggerFactory = LoggerFactory.Create(builder =>
        {
            builder
                .SetMinimumLevel(LogLevel.Trace)  // ここで最低ログレベル指定
                .AddFilter("Streamiz.Kafka.Net", LogLevel.Trace)
                .AddConsole();
        });
        var logger = loggerFactory.CreateLogger<StreamizRocksDbTests>();
        stream.StateChanged += (@old, @new) =>
        {
            logger.LogInformation($" StateChanged {@new}");
        };
        await RetryAsync(async () =>
        {
            await stream.StartAsync();
            await WaitUntilRunningAsync(stream);
        }, retries, 2000);
    }

    private static async Task ProduceWithRetryAsync<TKey, TValue>(string topic, TKey key, TValue value, CachedSchemaRegistryClient schemaRegistry, int retries = 3)
    {
        var producerConfig = new ProducerConfig { BootstrapServers = "127.0.0.1:39092" };
        using var producer = new ProducerBuilder<TKey, TValue>(producerConfig)
            .SetKeySerializer(new AvroSerializer<TKey>(schemaRegistry))
            .SetValueSerializer(new AvroSerializer<TValue>(schemaRegistry))
            .Build();

        await RetryAsync(async () =>
        {
            await producer.ProduceAsync(topic, new Message<TKey, TValue> { Key = key, Value = value });
        }, retries);

        producer.Flush(TimeSpan.FromSeconds(10));
    }

    private static async Task RetryAsync(Func<Task> action, int retries = 3, int delayMs = 1000)
    {
        for (var attempt = 0; attempt < retries; attempt++)
        {
            try
            {
                await action();
                return;
            }
            catch when (attempt < retries - 1)
            {
                await Task.Delay(delayMs);
            }
        }
    }

    private static async Task<TValue> RunAvroToRocksDbAsync<TKey, TValue>(string topic, string storeName, string applicationId, TKey key, TValue value)
    {
        var loggerFactory = LoggerFactory.Create(builder =>
        {
            builder
                .SetMinimumLevel(LogLevel.Trace)  // ここで最低ログレベル指定
                .AddFilter("Streamiz.Kafka.Net", LogLevel.Trace)
                .AddConsole();
        });
        await EnsureTopicAsync(topic);
        var builder = new StreamBuilder();
        builder.Stream<TKey, TValue>(topic)
               .ToTable(CreateAvroMaterialized<TKey, TValue>(storeName));

        var stateDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        var config = new StreamConfig<SchemaAvroSerDes<TKey>, SchemaAvroSerDes<TValue>>
        {
            ApplicationId = applicationId,
            BootstrapServers = "127.0.0.1:39092",
            SchemaRegistryUrl = "http://127.0.0.1:18081",
            StateDir = stateDir,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            Logger = loggerFactory
        };

        var stream = new KafkaStream(builder.Build(), config);
        try
        {
            await StartWithRetryAsync(stream);

            var schemaConfig = new SchemaRegistryConfig { Url = "http://127.0.0.1:18081" };
            using var schemaRegistry = new CachedSchemaRegistryClient(schemaConfig);
            await ProduceWithRetryAsync(topic, key, value, schemaRegistry);

            await Task.Delay(TimeSpan.FromSeconds(5));
            var store = stream.Store(StoreQueryParameters.FromNameAndType(storeName, QueryableStoreTypes.KeyValueStore<TKey, TValue>()));
            return store.Get(key);
        }
        finally
        {
            //stream.Dispose();
            //Directory.Delete(stateDir, true);
        }
    }

    void OnStateChanged(object? _, KafkaStream.State s)
    {
        // 必要な最小の処理だけ（例：ログ）
        _logger?.LogInformation("Streams state -> {State}", s);
    }
    private ILogger? _logger;
    #region test1
    [Fact]
    public async Task String_To_RocksDb()
    {
        const string topic = "streamiz-string";
        const string storeName = "string-store";
        await EnsureTopicAsync(topic);
        var builder = new StreamBuilder();
        builder.Stream<string, string>(topic)
               .ToTable(Materialized<string, string, IKeyValueStore<Bytes, byte[]>>.Create<StringSerDes, StringSerDes>(storeName));
        var loggerFactory = LoggerFactory.Create(builder =>
        {
            builder
                .SetMinimumLevel(LogLevel.Trace)  // ここで最低ログレベル指定
                .AddFilter("Streamiz.Kafka.Net", LogLevel.Trace)
                .AddConsole();
        });
        var stateDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        var config = new StreamConfig<StringSerDes, StringSerDes>
        {
            ApplicationId = "string-test-app",
            BootstrapServers = "127.0.0.1:39092",
            StateDir = stateDir,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            Logger = loggerFactory
        };
        _logger = loggerFactory.CreateLogger<StringSerDes>();

        var stream = new KafkaStream(builder.Build(), config);
        try
        {
            stream.StateChanged += OnStateChanged;
            await stream.StartAsync();
            await WaitUntilRunningAsync(stream);

        }
        finally
        {
            stream.StateChanged -= OnStateChanged;
        }

        var producerConfig = new ProducerConfig { BootstrapServers = "127.0.0.1:39092" };
        using (var producer = new ProducerBuilder<string, string>(producerConfig).Build())
        {
            await producer.ProduceAsync(topic, new Message<string, string> { Key = "k1", Value = "v1" });
            producer.Flush(TimeSpan.FromSeconds(10));
        }

        await Task.Delay(TimeSpan.FromSeconds(5));
        var store = stream.Store(StoreQueryParameters.FromNameAndType(storeName, QueryableStoreTypes.KeyValueStore<string, string>()));
        Assert.Equal("v1", store.Get("k1"));

        stream.Dispose();
        Directory.Delete(stateDir, true);
    }

    [Fact]
    public async Task Bytes_To_RocksDb()
    {
        const string topic = "streamiz-bytes";
        const string storeName = "bytes-store";
        await EnsureTopicAsync(topic);
        var builder = new StreamBuilder();
        builder.Stream<byte[], byte[]>(topic)
               .ToTable(Materialized<byte[], byte[], IKeyValueStore<Bytes, byte[]>>.Create<ByteArraySerDes, ByteArraySerDes>(storeName));

        var stateDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        var config = new StreamConfig<ByteArraySerDes, ByteArraySerDes>
        {
            ApplicationId = "bytes-test-app",
            BootstrapServers = "127.0.0.1:39092",
            StateDir = stateDir,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        var stream = new KafkaStream(builder.Build(), config);
        await StartWithRetryAsync(stream);

        var key = new byte[] { 0x01 };
        var value = new byte[] { 0x02, 0x03 };
        var producerConfig = new ProducerConfig { BootstrapServers = "127.0.0.1:39092" };
        using (var producer = new ProducerBuilder<byte[], byte[]>(producerConfig).Build())
        {
            await producer.ProduceAsync(topic, new Message<byte[], byte[]> { Key = key, Value = value });
            producer.Flush(TimeSpan.FromSeconds(10));
        }

        await Task.Delay(TimeSpan.FromSeconds(5));
        var store = stream.Store(StoreQueryParameters.FromNameAndType(storeName, QueryableStoreTypes.KeyValueStore<byte[], byte[]>()));
        var stored = store.Get(key);
        Assert.True(stored.SequenceEqual(value));

        stream.Dispose();
        Directory.Delete(stateDir, true);
    }

    [Fact]
    public async Task Avro_To_RocksDb()
    {
        const string topic = "streamiz-avro";
        const string storeName = "avro-store";
        await EnsureTopicAsync(topic);
        var builder = new StreamBuilder();
        builder.Stream<string, User>(topic)
               .ToTable(Materialized<string, User, IKeyValueStore<Bytes, byte[]>>.Create<StringSerDes, SchemaAvroSerDes<User>>(storeName));

        var stateDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        var config = new StreamConfig<StringSerDes, SchemaAvroSerDes<User>>
        {
            ApplicationId = "avro-test-app",
            BootstrapServers = "127.0.0.1:39092",
            SchemaRegistryUrl = "http://127.0.0.1:18081",
            StateDir = stateDir,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        var stream = new KafkaStream(builder.Build(), config);
        await StartWithRetryAsync(stream);

        var producerConfig = new ProducerConfig { BootstrapServers = "127.0.0.1:39092" };
        var schemaConfig = new SchemaRegistryConfig { Url = "http://127.0.0.1:18081" };
        using var schemaRegistry = new CachedSchemaRegistryClient(schemaConfig);
        using (var producer = new ProducerBuilder<string, User>(producerConfig)
            .SetKeySerializer(Serializers.Utf8)
            .SetValueSerializer(new AvroSerializer<User>(schemaRegistry))
            .Build())
        {
            var user = new User { name = "alice", age = 30 };
            await producer.ProduceAsync(topic, new Message<string, User> { Key = "u1", Value = user });
            producer.Flush(TimeSpan.FromSeconds(10));
        }

        await Task.Delay(TimeSpan.FromSeconds(5));
        var store = stream.Store(StoreQueryParameters.FromNameAndType(storeName, QueryableStoreTypes.KeyValueStore<string, User>()));

        var stored = store.Get("u1");
        Assert.Equal("alice", stored.name);
        Assert.Equal(30, stored.age);

        stream.Dispose();
        Directory.Delete(stateDir, true);
    }
    #endregion
    [Fact]
    public async Task AvroKey_To_RocksDb()
    {
        var key = new User { name = "key1", age = 1 };
        var value = new User { name = "alice", age = 30 };
        var stored = await RunAvroToRocksDbAsync("streamiz-avro-key", "avro-key-store", "avro-key-test-app", key, value);
        Assert.Equal("alice", stored.name);
        Assert.Equal(30, stored.age);
    }

    [Fact]
    public async Task AvroKeyValueDifferentTypes_To_RocksDb()
    {
        var key = new User { name = "key1", age = 1 };
        var value = new Address { street = "main", zip = 12345 };
        var stored = await RunAvroToRocksDbAsync<User, Address>("streamiz-avro-key-different", "avro-key-different-store", "avro-key-different-test-app", key, value);
        Assert.Equal("main", stored.street);
        Assert.Equal(12345, stored.zip);
    }
    [Fact]
    public async Task AvroKeyValueDifferentTypes2_To_RocksDb()
    {
        var key = new User { name = "key1", age = 1 };
        var value = new User { name = "alice", age = 30 };
        var address = new Address { street = "main", zip = 12345 };
        var stored1 = await RunAvroToRocksDbAsync("streamiz-avro-key", "avro-key-store", "avro-key-test-app", key, value);
        var stored2 = await RunAvroToRocksDbAsync<User, Address>("streamiz-avro-key-different", "avro-key-different-store", "avro-key-different-test-app", key, address);
        Assert.Equal("alice", stored1.name);
        Assert.Equal(30, stored1.age);
        Assert.Equal("main", stored2.street);
        Assert.Equal(12345, stored2.zip);
    }
}
