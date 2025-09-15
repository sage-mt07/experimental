using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry.Serdes;
using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Configuration.Abstractions;
using Kafka.Ksql.Linq.Configuration.Messaging;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Dlq;
using Kafka.Ksql.Linq.Core.Extensions;
using Kafka.Ksql.Linq.Mapping;
using Kafka.Ksql.Linq.Messaging.Producers;
using Kafka.Ksql.Linq.Runtime.Heartbeat;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using ConfluentSchemaRegistry = Confluent.SchemaRegistry;

namespace Kafka.Ksql.Linq.Messaging.Consumers;

internal class KafkaConsumerManager : IDisposable
{
    private readonly KsqlDslOptions _options;
    private readonly ILogger? _logger;
    private readonly Lazy<ConfluentSchemaRegistry.ISchemaRegistryClient> _schemaRegistryClient;
    private readonly ConcurrentDictionary<Type, EntityModel> _entityModels;
    private readonly MappingRegistry _mappingRegistry;
    private readonly DlqOptions _dlq;
    private readonly IRateLimiter _limiter;
    private readonly IDlqProducer _dlqProducer;
    private readonly ICommitManager _commitManager;
    private readonly ILeadershipFlag _flag;
    private bool _disposed;
    private string? _hbTopicName;
    private IConsumer<Ignore, Ignore>? _hbLeaderConsumer;
    private CancellationTokenSource? _hbLeaderCts;
    private int _leaderLoopState;

    public event Action<IReadOnlyList<TopicPartition>>? PartitionsAssigned;
    public event Action<IReadOnlyList<TopicPartitionOffset>>? PartitionsRevoked;
    public bool IsLeader => _flag.CanSend;

#pragma warning disable CS0067 // Event is never used
    public event Func<byte[]?, Exception, string, int, long, DateTime, Headers?, string, string, Task>? DeserializationError;
#pragma warning restore CS0067

    public KafkaConsumerManager(
        MappingRegistry mapping,
        IOptions<KsqlDslOptions> options,
        ConcurrentDictionary<Type, EntityModel> entityModels,
        IDlqProducer dlqProducer,
        ICommitManager commitManager,
        ILeadershipFlag flag,
        ILoggerFactory? loggerFactory = null,
        IRateLimiter? limiter = null)
    {
        _mappingRegistry = mapping;
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        DefaultValueBinder.ApplyDefaults(_options);
        _entityModels = entityModels ?? new();
        _dlqProducer = dlqProducer;
        _commitManager = commitManager;
        _flag = flag;
        _logger = loggerFactory?.CreateLogger<KafkaConsumerManager>();
        _schemaRegistryClient = new Lazy<ConfluentSchemaRegistry.ISchemaRegistryClient>(CreateSchemaRegistryClient);
        _dlq = _options.DlqOptions;
        _limiter = limiter ?? new SimpleRateLimiter(_dlq.MaxPerSecond);
    }



    public async IAsyncEnumerable<(TPOCO, Dictionary<string, string>, MessageMeta)> ConsumeAsync<TPOCO>(
        bool fromBeginning = false,
        bool autoCommit = true,
        [EnumeratorCancellation] CancellationToken cancellationToken = default) where TPOCO : class
    {
        var model = GetEntityModel<TPOCO>();
        var topic = model.GetTopicName();
        var mapping = _mappingRegistry.GetMapping(typeof(TPOCO));
        var config = BuildConsumerConfig(topic, null, model.GroupId, autoCommit);

        // Support key-less entities by falling back to Ignore for TKey
        var keyType = mapping.AvroKeyType ?? typeof(Confluent.Kafka.Ignore);
        var method = typeof(KafkaConsumerManager)
            .GetMethod(nameof(ConsumeInternal), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
            .MakeGenericMethod(keyType, mapping.AvroValueType!, typeof(TPOCO));

        var enumerable = (IAsyncEnumerable<(TPOCO, Dictionary<string, string>, MessageMeta)>)method
            .Invoke(this, new object?[] { topic, config, mapping, fromBeginning, model.Partitions, cancellationToken })!;

        await foreach (var item in enumerable.WithCancellation(cancellationToken))
            yield return item;
    }

    public virtual void StartLeaderElectionSafe(string? topic = null, string? groupId = null, string? instanceId = null, CancellationToken ct = default)
    {
        if (Interlocked.CompareExchange(ref _leaderLoopState, 1, 0) != 0) return;
        StartLeaderElection(topic, groupId, instanceId, ct);
    }

    private void StartLeaderElection(string? topic, string? groupId, string? instanceId, CancellationToken ct)
    {
        _hbTopicName = topic ?? _options.Heartbeat.Topic;
        var sub = new KafkaSubscriptionOptions { GroupId = groupId ?? _options.Heartbeat.LeaderElection.GroupId };
        var config = BuildConsumerConfig(_hbTopicName, sub, null, true);
        config.GroupInstanceId = instanceId ?? _options.Heartbeat.LeaderElection.InstanceId;

        _hbLeaderConsumer = new ConsumerBuilder<Ignore, Ignore>(config)
            .SetPartitionsAssignedHandler((_, parts) => HandlePartitionsAssigned(parts))
            .SetPartitionsRevokedHandler((_, parts) => HandlePartitionsRevoked(parts))
            .Build();
        _hbLeaderConsumer.Assign(new TopicPartition(_hbTopicName, new Partition(0)));

        _hbLeaderCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var token = _hbLeaderCts.Token;
        Task.Run(() =>
        {
            try
            {
                while (!token.IsCancellationRequested)
                    _hbLeaderConsumer.Consume(token);
            }
            catch (OperationCanceledException) { }
            finally
            {
                _hbLeaderConsumer.Close();
                _hbLeaderConsumer.Dispose();
            }
        }, token);
    }

    private async IAsyncEnumerable<(TPOCO, Dictionary<string, string>, MessageMeta)> ConsumeInternal<TKey, TValue, TPOCO>(
        string topicName,
        ConsumerConfig config,
        KeyValueTypeMapping mapping,
        bool fromBeginning,
        int partitions,
        [EnumeratorCancellation] CancellationToken cancellationToken)
        where TKey : class where TValue : class where TPOCO : class
    {
        using var consumer = CreateConsumer<TKey, TValue>(config);
        if (fromBeginning)
        {
            var tps = new List<TopicPartitionOffset>(partitions);
            for (var i = 0; i < partitions; i++)
                tps.Add(new TopicPartitionOffset(topicName, new Partition(i), new Offset(0)));
            consumer.Assign(tps);
            consumer.Commit(tps);
        }
        consumer.Subscribe(topicName);
        try { Console.WriteLine($"[ConsumeInternal] subscribed topic={topicName} group={config.GroupId} autoCommit={config.EnableAutoCommit}"); } catch {}
        if (config.EnableAutoCommit != true)
            _commitManager.Bind(typeof(TPOCO), topicName, consumer);

        while (!cancellationToken.IsCancellationRequested)
        {
            ConsumeResult<TKey, TValue>? result;
            try
            {
                result = consumer.Consume(cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            if (result == null || result.IsPartitionEOF)
                continue;

            TPOCO entity;
            Dictionary<string, string> headers;
            MessageMeta meta;
            try
            {
                entity = (TPOCO)mapping.CombineFromAvroKeyValue(result.Message.Key, result.Message.Value!, typeof(TPOCO));
                headers = new Dictionary<string, string>();
                if (result.Message.Headers != null)
                {
                    foreach (var h in result.Message.Headers)
                        headers[h.Key] = System.Text.Encoding.UTF8.GetString(h.GetValueBytes());
                }

                meta = new MessageMeta(
                    Topic: result.Topic,
                    Partition: result.Partition,
                    Offset: result.Offset,
                    TimestampUtc: result.Message.Timestamp.UtcDateTime,
                    SchemaIdKey: TryGetSchemaId(result.Message.Key as byte[]),
                    SchemaIdValue: TryGetSchemaId(result.Message.Value as byte[]),
                    KeyIsNull: result.Message.Key is null,
                    HeaderAllowList: ExtractAllowedHeaders(result.Message.Headers, _dlq.HeaderAllowList, _dlq.HeaderValueMaxLength)
                );
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex,
                    "Consume mapping failed. Topic={Topic}, Partition={Partition}, Offset={Offset}, Error={ErrorType}: {Message}",
                    result.Topic, result.Partition.Value, result.Offset.Value, ex.GetType().Name, ex.Message);
                await HandleMappingException(result, ex, _dlqProducer, consumer, _dlq, _limiter, cancellationToken).ConfigureAwait(false);
                continue;
            }

            yield return (entity, headers, meta);
            await Task.CompletedTask;
        }
    }

    internal static async Task HandleMappingException<TKey, TValue>(
        ConsumeResult<TKey, TValue> result,
        Exception ex,
        IDlqProducer dlqProducer,
        IConsumer<TKey, TValue> consumer,
        DlqOptions options,
        IRateLimiter limiter,
        CancellationToken cancellationToken)
        where TKey : class where TValue : class
    {
        if (options.EnableForDeserializationError && DlqGuard.ShouldSend(options, limiter, ex.GetType()))
        {
            var allowHeaders = ExtractAllowedHeaders(result.Message.Headers, options.HeaderAllowList, options.HeaderValueMaxLength);
            var env = DlqEnvelopeFactory.From(result, ex,
                options.ApplicationId, options.ConsumerGroup, options.Host, allowHeaders,
                options.ErrorMessageMaxLength, options.StackTraceMaxLength, options.NormalizeStackTraceWhitespace);
            await dlqProducer.ProduceAsync(env, cancellationToken).ConfigureAwait(false);
        }
        consumer.Commit(result);
    }

    private IConsumer<TKey, TValue> CreateConsumer<TKey, TValue>(ConsumerConfig config)
        where TKey : class where TValue : class
    {
        // When using Ignore as TKey (key-less topics), do not attach Avro deserializer for the key.
        if (typeof(TKey) == typeof(Confluent.Kafka.Ignore))
        {
            return (IConsumer<TKey, TValue>)(object)new ConsumerBuilder<Confluent.Kafka.Ignore, TValue>(config)
                .SetValueDeserializer(new AvroDeserializer<TValue>(_schemaRegistryClient.Value).AsSyncOverAsync())
                .Build();
        }

        return new ConsumerBuilder<TKey, TValue>(config)
            .SetKeyDeserializer(new AvroDeserializer<TKey>(_schemaRegistryClient.Value).AsSyncOverAsync())
            .SetValueDeserializer(new AvroDeserializer<TValue>(_schemaRegistryClient.Value).AsSyncOverAsync())
            .Build();
    }

    private void HandlePartitionsAssigned(IReadOnlyList<TopicPartition> parts)
    {
        var owns0 = parts.Any(p => p.Topic == _hbTopicName && p.Partition.Value == 0);
        if (owns0) _flag.Enable();
        PartitionsAssigned?.Invoke(parts);
    }

    private void HandlePartitionsRevoked(IReadOnlyList<TopicPartitionOffset> parts)
    {
        if (parts.Any(p => p.Topic == _hbTopicName))
            _flag.Disable();
        PartitionsRevoked?.Invoke(parts);
    }

    private ConfluentSchemaRegistry.ISchemaRegistryClient CreateSchemaRegistryClient()
    {
        var cfg = new ConfluentSchemaRegistry.SchemaRegistryConfig { Url = _options.SchemaRegistry.Url };
        _logger.LogClientConfig("schema-registry(consumer)", cfg);
        return new ConfluentSchemaRegistry.CachedSchemaRegistryClient(cfg);
    }

    private EntityModel GetEntityModel<T>() where T : class
    {
        if (_entityModels.TryGetValue(typeof(T), out var model))
            return model;
        throw new InvalidOperationException($"Entity model not found for {typeof(T).Name}");
    }

    private ConsumerConfig BuildConsumerConfig(string topicName, KafkaSubscriptionOptions? subscriptionOptions, string? modelGroupId, bool autoCommit)
    {
        var hasConfig = _options.Topics.TryGetValue(topicName, out var cfg);
        TopicSection topicConfig = hasConfig && cfg is not null ? cfg : new TopicSection();
        DefaultValueBinder.ApplyDefaults(topicConfig);

        string? groupId = null;
        if (hasConfig && !string.IsNullOrWhiteSpace(topicConfig.Consumer!.GroupId))
            groupId = topicConfig.Consumer.GroupId;
        else if (!string.IsNullOrWhiteSpace(subscriptionOptions?.GroupId))
            groupId = subscriptionOptions.GroupId;
        else if (!string.IsNullOrWhiteSpace(modelGroupId))
            groupId = modelGroupId;

        DefaultValueBinder.ApplyDefaults(topicConfig.Consumer!);
        if (string.IsNullOrWhiteSpace(groupId))
            groupId = topicConfig.Consumer.GroupId;
        // Respect the caller's intent: prefer the explicit autoCommit flag from the API
        // Topic configuration may provide defaults, but should not override the method parameter.
        var enableAutoCommit = autoCommit;

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _options.Common.BootstrapServers,
            ClientId = _options.Common.ClientId,
            GroupId = groupId,
            AutoOffsetReset = Enum.Parse<AutoOffsetReset>(topicConfig.Consumer.AutoOffsetReset),
            EnableAutoCommit = enableAutoCommit,
            AutoCommitIntervalMs = topicConfig.Consumer.AutoCommitIntervalMs,
            SessionTimeoutMs = topicConfig.Consumer.SessionTimeoutMs,
            HeartbeatIntervalMs = topicConfig.Consumer.HeartbeatIntervalMs,
            MaxPollIntervalMs = topicConfig.Consumer.MaxPollIntervalMs,
            FetchMinBytes = topicConfig.Consumer.FetchMinBytes,
            FetchMaxBytes = topicConfig.Consumer.FetchMaxBytes,
            IsolationLevel = Enum.Parse<IsolationLevel>(topicConfig.Consumer.IsolationLevel)
        };

        try { Console.WriteLine($"[ConsumerConfig] topic={topicName} group={groupId} enableAutoCommit={enableAutoCommit} autoOffsetReset={topicConfig.Consumer.AutoOffsetReset}"); } catch {}

        if (!hasConfig && subscriptionOptions != null)
        {
            if (subscriptionOptions.SessionTimeout.HasValue)
                consumerConfig.SessionTimeoutMs = (int)subscriptionOptions.SessionTimeout.Value.TotalMilliseconds;
            if (subscriptionOptions.HeartbeatInterval.HasValue)
                consumerConfig.HeartbeatIntervalMs = (int)subscriptionOptions.HeartbeatInterval.Value.TotalMilliseconds;
            if (subscriptionOptions.MaxPollInterval.HasValue)
                consumerConfig.MaxPollIntervalMs = (int)subscriptionOptions.MaxPollInterval.Value.TotalMilliseconds;
            if (subscriptionOptions.AutoOffsetReset.HasValue)
                consumerConfig.AutoOffsetReset = subscriptionOptions.AutoOffsetReset.Value;
        }

        if (_options.Common.SecurityProtocol != SecurityProtocol.Plaintext)
        {
            consumerConfig.SecurityProtocol = _options.Common.SecurityProtocol;
            if (_options.Common.SaslMechanism.HasValue)
            {
                consumerConfig.SaslMechanism = _options.Common.SaslMechanism.Value;
                consumerConfig.SaslUsername = _options.Common.SaslUsername;
                consumerConfig.SaslPassword = _options.Common.SaslPassword;
            }

            if (!string.IsNullOrEmpty(_options.Common.SslCaLocation))
            {
                consumerConfig.SslCaLocation = _options.Common.SslCaLocation;
                consumerConfig.SslCertificateLocation = _options.Common.SslCertificateLocation;
                consumerConfig.SslKeyLocation = _options.Common.SslKeyLocation;
                consumerConfig.SslKeyPassword = _options.Common.SslKeyPassword;
            }
        }

        foreach (var kvp in topicConfig.Consumer.AdditionalProperties)
            consumerConfig.Set(kvp.Key, kvp.Value);
        _logger.LogClientConfig($"consumer:{topicName}", consumerConfig, topicConfig.Consumer.AdditionalProperties);
        return consumerConfig;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        try { _hbLeaderCts?.Cancel(); } catch { }
        try { _hbLeaderConsumer?.Close(); _hbLeaderConsumer?.Dispose(); } catch { }
        _flag.Disable();

        if (_schemaRegistryClient.IsValueCreated)
            _schemaRegistryClient.Value.Dispose();
    }

    private static int? TryGetSchemaId(byte[]? payload)
    {
        if (payload is { Length: >= 5 } && payload[0] == 0)
            return System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(payload.AsSpan(1, 4));
        return null;
    }

    private static System.Collections.Generic.IReadOnlyDictionary<string, string> ExtractAllowedHeaders(
        Headers? headers, System.Collections.Generic.IEnumerable<string> allowList, int maxLen = 1024)
    {
        var dict = new System.Collections.Generic.Dictionary<string, string>(System.StringComparer.OrdinalIgnoreCase);
        if (headers is null) return dict;

        var set = allowList is System.Collections.Generic.HashSet<string> hs ? hs :
                  new System.Collections.Generic.HashSet<string>(allowList ?? System.Array.Empty<string>(), System.StringComparer.OrdinalIgnoreCase);

        foreach (var h in headers)
        {
            if (!set.Contains(h.Key)) continue;
            var bytes = h.GetValueBytes() ?? System.Array.Empty<byte>();
            string val;
            try
            {
                val = System.Text.Encoding.UTF8.GetString(bytes);
                if (!System.Text.Encoding.UTF8.GetBytes(val).AsSpan().SequenceEqual(bytes))
                    val = "base64:" + System.Convert.ToBase64String(bytes);
            }
            catch
            {
                val = "base64:" + System.Convert.ToBase64String(bytes);
            }
            if (val.Length > maxLen) val = val[..maxLen];
            dict[h.Key] = val;
        }
        return dict;
    }
}
