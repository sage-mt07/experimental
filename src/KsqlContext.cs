using Confluent.Kafka;
using Kafka.Ksql.Linq.Cache.Core;
using Kafka.Ksql.Linq.Cache.Extensions;
using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Dlq;
using Kafka.Ksql.Linq.Core.Extensions;
using Kafka.Ksql.Linq.Infrastructure.Admin;
using Kafka.Ksql.Linq.Infrastructure.KsqlDb;
using Kafka.Ksql.Linq.Mapping;
using Kafka.Ksql.Linq.Messaging.Consumers;
using Kafka.Ksql.Linq.Messaging.Producers;
using Kafka.Ksql.Linq.Query.Abstractions;
using Kafka.Ksql.Linq.Runtime.Heartbeat;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ConfluentSchemaRegistry = Confluent.SchemaRegistry;

namespace Kafka.Ksql.Linq;
/// <summary>
/// KsqlContext that integrates the Core layer.
/// Design rationale: inherits core abstractions and integrates higher-level features.
/// </summary>
public abstract partial class KsqlContext : IKsqlContext
{
    private KafkaProducerManager _producerManager = null!;
    private readonly ConcurrentDictionary<Type, EntityModel> _entityModels = new();
    private readonly Dictionary<Type, object> _entitySets = new();
    private readonly Dictionary<Type, Configuration.ResolvedEntityConfig> _resolvedConfigs = new();
    private bool _disposed = false;
    private KafkaConsumerManager _consumerManager = null!;
    private IDlqProducer _dlqProducer = null!;
    private ICommitManager _commitManager = null!;
    private Lazy<ConfluentSchemaRegistry.ISchemaRegistryClient> _schemaRegistryClient = null!;
    private IKsqlDbClient _ksqlDbClient = null!;
    private Core.Dlq.IDlqClient _dlqClient = null!;
    private IRateLimiter _dlqLimiter = null!;
    private ILeadershipFlag _leaderFlag = null!;
    private HeartbeatRunner? _hbRunner;
    private IMarketScheduleProvider _marketScheduleProvider = null!;
    private Task? _msRefreshTask;
    private Func<DateTime> _now = () => DateTime.UtcNow;
    private Func<TimeSpan, CancellationToken, Task> _delay = (t, ct) => Task.Delay(t, ct);

    private KafkaAdminService _adminService = null!;
    private readonly KsqlDslOptions _dslOptions;
    private TableCacheRegistry? _cacheRegistry;
    private readonly MappingRegistry _mappingRegistry = new();
    private ILogger _logger = null!;
    private ILoggerFactory? _loggerFactory;

    internal ILogger Logger => _logger;

    private static readonly System.Reflection.Emit.ModuleBuilder _derivedModule = System.Reflection.Emit.AssemblyBuilder
        .DefineDynamicAssembly(new System.Reflection.AssemblyName("KafkaKsqlLinq.Derived"), System.Reflection.Emit.AssemblyBuilderAccess.Run)
        .DefineDynamicModule("Main");
    private static readonly Dictionary<string, Type> _derivedTypes = new();



    /// <summary>
    /// Hook to decide whether schema registration should be skipped for tests
    /// </summary>
    protected virtual bool SkipSchemaRegistration => false;

    public const string DefaultSectionName = "KsqlDsl";

    protected KsqlContext(IConfiguration configuration, ILoggerFactory? loggerFactory = null)
        : this(configuration, DefaultSectionName, loggerFactory)
    {
    }

    protected KsqlContext(IConfiguration configuration, string sectionName, ILoggerFactory? loggerFactory = null)
    {
        _dslOptions = new KsqlDslOptions();
        configuration.GetSection(sectionName).Bind(_dslOptions);
        DefaultValueBinder.ApplyDefaults(_dslOptions);

        InitializeCore(loggerFactory);

    }

    protected KsqlContext(KsqlDslOptions options, ILoggerFactory? loggerFactory = null)
    {
        _dslOptions = options;
        DefaultValueBinder.ApplyDefaults(_dslOptions);
        InitializeCore(loggerFactory);
    }

    private void InitializeCore(ILoggerFactory? loggerFactory)
    {
        // Configure only per-property decimal overrides; global precision/scale options removed from docs
        DecimalPrecisionConfig.Configure(_dslOptions.Decimals);

        _schemaRegistryClient = new Lazy<ConfluentSchemaRegistry.ISchemaRegistryClient>(CreateSchemaRegistryClient);
        _ksqlDbClient = new KsqlDbClient(GetDefaultKsqlDbUrl(), loggerFactory?.CreateLogger<KsqlDbClient>());

        _loggerFactory = loggerFactory;
        _logger = _loggerFactory.CreateLoggerOrNull<KsqlContext>();


        _adminService = new KafkaAdminService(
        Microsoft.Extensions.Options.Options.Create(_dslOptions),
        _loggerFactory);
        InitializeEntityModels();
        try
        {
            _producerManager = new KafkaProducerManager(_mappingRegistry,
                 Microsoft.Extensions.Options.Options.Create(_dslOptions),
                 _loggerFactory);
            _dlqProducer = new Kafka.Ksql.Linq.Messaging.Producers.DlqProducer(_producerManager, _dslOptions.DlqTopicName);

            _commitManager = new ManualCommitManager(_loggerFactory?.CreateLogger<Messaging.Consumers.ManualCommitManager>());

            ConfigureModel();
            ResolveEntityConfigurations();




            if (!SkipSchemaRegistration)
            {
                InitializeWithSchemaRegistration();
            }
            this.UseTableCache(_dslOptions, _loggerFactory);
            _cacheRegistry = this.GetTableCacheRegistry();

            _dlqLimiter = new SimpleRateLimiter(_dslOptions.DlqOptions.MaxPerSecond);

            _leaderFlag = new LeadershipFlag();
            _marketScheduleProvider = new MarketScheduleProvider(_mappingRegistry);
            _consumerManager = new KafkaConsumerManager(_mappingRegistry,
                Microsoft.Extensions.Options.Options.Create(_dslOptions),
                _entityModels,
                _dlqProducer,
                _commitManager,
                _leaderFlag,
                _loggerFactory,
                _dlqLimiter);

            _dlqClient = new Core.Dlq.DlqClient(_dslOptions, _consumerManager, _loggerFactory);

        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"KsqlContext initialization failed: {ex.Message} ");
            throw;
        }
    }

    protected virtual void OnModelCreating(IModelBuilder modelBuilder) { }

    /// <summary>
    /// OnModelCreating → execute automatic schema registration flow
    /// </summary>
    private ConfluentSchemaRegistry.ISchemaRegistryClient CreateSchemaRegistryClient()
    {
        var options = _dslOptions.SchemaRegistry;
        var config = new ConfluentSchemaRegistry.SchemaRegistryConfig
        {
            Url = options.Url,
            MaxCachedSchemas = options.MaxCachedSchemas,
            RequestTimeoutMs = options.RequestTimeoutMs
        };

        return new ConfluentSchemaRegistry.CachedSchemaRegistryClient(config);
    }


    private Uri GetDefaultKsqlDbUrl()
    {
        if (!string.IsNullOrWhiteSpace(_dslOptions.KsqlDbUrl) &&
            Uri.TryCreate(_dslOptions.KsqlDbUrl, UriKind.Absolute, out var configured))
        {
            return configured;
        }

        var schemaUrl = _dslOptions.SchemaRegistry.Url;
        if (!string.IsNullOrWhiteSpace(schemaUrl) &&
            Uri.TryCreate(schemaUrl, UriKind.Absolute, out var schemaUri))
        {
            var port = schemaUri.IsDefaultPort || schemaUri.Port == 8081 ? 8088 : schemaUri.Port;
            return new Uri($"{schemaUri.Scheme}://{schemaUri.Host}:{port}");
        }

        // Default to localhost if nothing configured (test-friendly)
        return new Uri("http://localhost:8088");
    }
    private HttpClient CreateClient()
    {
        return new HttpClient { BaseAddress = GetDefaultKsqlDbUrl() };
    }

    public Task<KsqlDbResponse> ExecuteStatementAsync(string statement)
    {
        _logger?.LogInformation("ksql execute: {Kind} SQL={Preview}", "EXECUTE", Preview(statement));
        return _ksqlDbClient.ExecuteStatementAsync(statement);
    }

    public Task<KsqlDbResponse> ExecuteExplainAsync(string ksql)
    {
        var rewritten = TryQualifySimpleJoin(ksql);
        _logger?.LogInformation("ksql execute: {Kind} SQL={Preview}", "EXPLAIN", Preview(rewritten));
        return _ksqlDbClient.ExecuteExplainAsync(rewritten);
    }

    public Task<int> QueryStreamCountAsync(string sql, TimeSpan? timeout = null)
    {
        _logger?.LogInformation("ksql execute: {Kind} SQL={Preview}", "QUERY_STREAM_COUNT", Preview(sql));
        return _ksqlDbClient.ExecuteQueryStreamCountAsync(sql, timeout);
    }

    public Task<int> QueryCountAsync(string sql, TimeSpan? timeout = null)
    {
        _logger?.LogInformation("ksql execute: {Kind} SQL={Preview}", "QUERY_PULL_COUNT", Preview(sql));
        return _ksqlDbClient.ExecutePullQueryCountAsync(sql, timeout);
    }

    private static string Preview(string? sql, int max = 120)
    {
        if (string.IsNullOrWhiteSpace(sql)) return string.Empty;
        sql = sql.Replace("\n", " ").Replace("\r", " ").Trim();
        return sql.Length <= max ? sql : sql.Substring(0, max) + "...";
    }

    public Task<System.Collections.Generic.List<object?[]>> QueryRowsAsync(string sql, TimeSpan? timeout = null)
    {
        return ((Infrastructure.KsqlDb.KsqlDbClient)_ksqlDbClient).ExecutePullQueryRowsAsync(sql, timeout);
    }

    private static string TryQualifySimpleJoin(string ksql)
    {
        try
        {
            var text = ksql;
            var fromIdx = text.IndexOf("FROM ", StringComparison.OrdinalIgnoreCase);
            var joinIdx = text.IndexOf(" JOIN ", StringComparison.OrdinalIgnoreCase);
            if (fromIdx < 0 || joinIdx < 0 || !(fromIdx < joinIdx)) return ksql;

            string ReadIdent(string s, int start)
            {
                int i = start;
                while (i < s.Length && char.IsWhiteSpace(s[i])) i++;
                int j = i;
                while (j < s.Length && (char.IsLetterOrDigit(s[j]) || s[j] == '_' || s[j] == '.')) j++;
                return s.Substring(i, Math.Max(0, j - i)).Trim();
            }

            var left = ReadIdent(text, fromIdx + 5);
            var right = ReadIdent(text, joinIdx + 6);
            if (string.IsNullOrEmpty(left) || string.IsNullOrEmpty(right)) return ksql;

            // qualify a simple equality ON clause with bare identifiers
            var pattern = new System.Text.RegularExpressions.Regex(@"ON\s*\(\s*([A-Za-z_][\w]*)\s*=\s*([A-Za-z_][\w]*)\s*\)", System.Text.RegularExpressions.RegexOptions.IgnoreCase);
            var replaced = pattern.Replace(text, m => $"ON ({left}.{m.Groups[1].Value} = {right}.{m.Groups[2].Value})");
            if (replaced.IndexOf(" JOIN ", StringComparison.OrdinalIgnoreCase) >= 0 &&
                replaced.IndexOf("EMIT CHANGES", StringComparison.OrdinalIgnoreCase) < 0)
            {
                // insert EMIT CHANGES before trailing ';' if present
                var semi = replaced.LastIndexOf(';');
                if (semi >= 0)
                    replaced = replaced.Substring(0, semi) + " EMIT CHANGES" + replaced.Substring(semi);
                else
                    replaced += " EMIT CHANGES";
            }
            if (replaced.IndexOf(" JOIN ", StringComparison.OrdinalIgnoreCase) >= 0 &&
                replaced.IndexOf(" WITHIN ", StringComparison.OrdinalIgnoreCase) < 0)
            {
                var re = new System.Text.RegularExpressions.Regex(@"JOIN\s+([A-Za-z_][\w]*)\s+ON\s*\(", System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                replaced = re.Replace(replaced, m => $"JOIN {m.Groups[1].Value} WITHIN 300 SECONDS ON (");
            }
            return replaced;
        }
        catch
        {
            return ksql;
        }
    }



    /// <summary>
    /// Core-level EventSet implementation (integrates higher-level services).
    /// </summary>
    protected virtual IEntitySet<T> CreateEntitySet<T>(EntityModel entityModel) where T : class
    {
        var model = EnsureEntityModel(typeof(T), entityModel);
        var baseSet = new EventSetWithServices<T>(this, model);
        if (model.GetExplicitStreamTableType() == StreamTableType.Table && model.EnableCache)
        {
            return new ReadCachedEntitySet<T>(this, model, null, baseSet);
        }
        return baseSet;
    }

    internal KafkaProducerManager GetProducerManager() => _producerManager;

    public async Task StartHeartbeatRunnerAsync(CancellationToken appStopping)
    {
        if (_hbRunner != null) return;
        if (!_entityModels.Values.Any(m => m.QueryModel?.HasTumbling() == true)) return;

        var scheduleType = _entityModels.Values
            .Select(m => m.QueryModel?.BasedOnType)
            .FirstOrDefault(t => t != null);
        if (scheduleType == null) return;
        var setMethod = GetType().GetMethod(nameof(Set))!.MakeGenericMethod(scheduleType);
        var set = setMethod.Invoke(this, null);
        if (set == null) throw new InvalidOperationException("MarketSchedule entity set not found");
        var rows = await ((dynamic)set).ToListAsync(appStopping);
        await _marketScheduleProvider.InitializeAsync(scheduleType, rows, appStopping);
        StartDailyRefresh(scheduleType, set, appStopping);

        _consumerManager.StartLeaderElectionSafe(
            _dslOptions.Heartbeat.Topic,
            _dslOptions.Heartbeat.LeaderElection.GroupId,
            _dslOptions.Heartbeat.LeaderElection.InstanceId,
            appStopping);

        var producer = new ProducerBuilder<byte[], byte[]>(new ProducerConfig
        {
            BootstrapServers = _dslOptions.Common.BootstrapServers
        }).Build();
        var sender = new KafkaHeartbeatSender(producer, _leaderFlag, _dslOptions.Heartbeat.Topic);
        var planner = new HeartbeatPlanner(_dslOptions.Heartbeat.Grace, Array.Empty<HeartbeatItem>(), _marketScheduleProvider);
        _hbRunner = new HeartbeatRunner(planner, sender, 0);
        _hbRunner.Start(appStopping);
    }

    private void StartDailyRefresh(Type scheduleType, object set, CancellationToken token)
    {
        if (_msRefreshTask != null) return;
        _msRefreshTask = Task.Run(async () =>
        {
            while (!token.IsCancellationRequested)
            {
                var now = _now();
                var next = new DateTime(now.Year, now.Month, now.Day, 0, 5, 0, DateTimeKind.Utc);
                if (now >= next) next = next.AddDays(1);
                await _delay(next - now, token);
                var fresh = await ((dynamic)set).ToListAsync(token);
                await _marketScheduleProvider.RefreshAsync(scheduleType, fresh, token);
            }
        }, token);
    }
    internal KafkaConsumerManager GetConsumerManager() => _consumerManager;
    internal IDlqProducer GetDlqProducer() => _dlqProducer;
    internal ICommitManager GetCommitManager() => _commitManager;
    internal DlqOptions DlqOptions => _dslOptions.DlqOptions;
    internal IRateLimiter DlqLimiter => _dlqLimiter;
    internal ConfluentSchemaRegistry.ISchemaRegistryClient GetSchemaRegistryClient() => _schemaRegistryClient.Value;
    internal MappingRegistry GetMappingRegistry() => _mappingRegistry;
    public Core.Dlq.IDlqClient Dlq => _dlqClient;

    /// <summary>
    /// Get the topic name from an entity type
    /// </summary>
    public string GetTopicName<T>()
    {
        var models = GetEntityModels();
        if (models.TryGetValue(typeof(T), out var model))
        {
            return (model.TopicName ?? typeof(T).Name).ToLowerInvariant();
        }
        return typeof(T).Name.ToLowerInvariant();
    }

    internal async Task<bool> IsEntityReadyAsync<T>(CancellationToken cancellationToken = default) where T : class
    {
        var models = GetEntityModels();
        if (!models.TryGetValue(typeof(T), out var model))
            return false;

        var statement = model.GetExplicitStreamTableType() == StreamTableType.Table
            ? "SHOW TABLES;"
            : "SHOW STREAMS;";

        var name = (model.TopicName ?? typeof(T).Name).ToUpperInvariant();
        var response = await ExecuteStatementAsync(statement);
        if (!response.IsSuccess)
            return false;

        try
        {
            using var doc = JsonDocument.Parse(response.Message);
            var listName = statement.Contains("TABLES") ? "tables" : "streams";
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

    public async Task WaitForEntityReadyAsync<T>(TimeSpan timeout, CancellationToken cancellationToken = default) where T : class
    {
        var start = DateTime.UtcNow;
        while (DateTime.UtcNow - start < timeout)
        {
            if (await IsEntityReadyAsync<T>(cancellationToken))
                return;

            await Task.Delay(100, cancellationToken);
        }

        throw new TimeoutException($"Entity {typeof(T).Name} not ready after {timeout}.");
    }




    protected virtual void Dispose(bool disposing)
    {
        if (!_disposed && disposing)
        {
            foreach (var entitySet in _entitySets.Values)
            {
                if (entitySet is IDisposable disposable)
                {
                    disposable.Dispose();
                }
            }
            _entitySets.Clear();
            _entityModels.Clear();
            _disposed = true;

            _producerManager?.Dispose();
            _consumerManager?.Dispose();
            _adminService?.Dispose();
            _cacheRegistry?.Dispose();

            if (_schemaRegistryClient.IsValueCreated)
            {
                _schemaRegistryClient.Value?.Dispose();
            }
            (_ksqlDbClient as IDisposable)?.Dispose();
        }
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    public async ValueTask DisposeAsync()
    {
        await DisposeAsyncCore();
        Dispose(false);
        GC.SuppressFinalize(this);
    }

    protected virtual async ValueTask DisposeAsyncCore()
    {
        foreach (var entitySet in _entitySets.Values)
        {
            if (entitySet is IAsyncDisposable asyncDisposable)
            {
                await asyncDisposable.DisposeAsync();
            }
            else if (entitySet is IDisposable disposable)
            {
                disposable.Dispose();
            }
        }
        _entitySets.Clear();

        _producerManager?.Dispose();
        _consumerManager?.Dispose();
        _adminService?.Dispose();
        _cacheRegistry?.Dispose();

        if (_schemaRegistryClient.IsValueCreated)
        {
            _schemaRegistryClient.Value?.Dispose();
        }
        (_ksqlDbClient as IDisposable)?.Dispose();

        await Task.CompletedTask;
    }

    public override string ToString()
    {
        return $"KafkaContextCore: {_entityModels.Count} entities, {_entitySets.Count} sets [schema auto-registration ready]";
    }
}
