using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Messaging.Consumers;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Kafka.Ksql.Linq.Core.Dlq;

internal class DlqClient : IDlqClient
{
    private readonly KsqlDslOptions _options;
    private readonly KafkaConsumerManager _consumerManager;
    private readonly ILogger? _logger;

    internal DlqClient(
        KsqlDslOptions options,
        KafkaConsumerManager consumerManager,
        ILoggerFactory? loggerFactory = null)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _consumerManager = consumerManager ?? throw new ArgumentNullException(nameof(consumerManager));
        _logger = loggerFactory?.CreateLogger<DlqClient>();
    }

    public async IAsyncEnumerable<DlqRecord> ReadAsync(
        DlqReadOptions? options = null,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        options ??= new DlqReadOptions();
        if (string.IsNullOrWhiteSpace(_options.DlqTopicName))
            throw new InvalidOperationException("DLQ topic is not configured.");

        await foreach (var (env, _, _) in _consumerManager.ConsumeAsync<Messaging.DlqEnvelope>(options.FromBeginning, autoCommit: true, cancellationToken: ct).WithCancellation(ct))
        {
            var record = await CreateRecordAsync(env, options);
            yield return record;
        }
    }

    private Task<DlqRecord> CreateRecordAsync(Messaging.DlqEnvelope env, DlqReadOptions opts)
    {
        var headers = env.Headers ?? new Dictionary<string, string>();
        var record = new DlqRecord(
            env.ErrorFingerprint,
            env.Topic,
            env.Partition,
            env.Offset,
            env.TimestampUtc,
            Array.Empty<byte>(),
            null,
            env.PayloadFormatValue,
            env.SchemaIdValue,
            null,
            null,
            headers,
            env.ErrorType,
            env.ErrorMessageShort,
            env.StackTraceShort);
        return Task.FromResult(record);
    }

}
