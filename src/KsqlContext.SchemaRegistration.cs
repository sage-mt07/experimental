using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Core.Extensions;
using Kafka.Ksql.Linq.Query.Abstractions;
using Kafka.Ksql.Linq.Query.Adapters;
using Kafka.Ksql.Linq.Query.Analysis;
using Kafka.Ksql.Linq.Query.Ddl;
using Kafka.Ksql.Linq.SchemaRegistryTools;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using System.Linq.Expressions;
using ConfluentSchemaRegistry = Confluent.SchemaRegistry;

namespace Kafka.Ksql.Linq;

public abstract partial class KsqlContext
{
    private void InitializeWithSchemaRegistration()
    {
        // Register schemas and materialize entities if new
        using (Kafka.Ksql.Linq.Core.Modeling.ModelCreatingScope.Enter())
        {
            RegisterSchemasAndMaterializeAsync().GetAwaiter().GetResult();
        }

        var tableTopics = _ksqlDbClient.GetTableTopicsAsync().GetAwaiter().GetResult();
        _cacheRegistry?.RegisterEligibleTables(_entityModels.Values, tableTopics);

        // Verify Kafka connectivity
        ValidateKafkaConnectivity();
        EnsureKafkaReadyAsync().GetAwaiter().GetResult();
    }
    private async Task EnsureKafkaReadyAsync()
    {
        try
        {
            // Auto-create DLQ topic
            await _adminService.EnsureDlqTopicExistsAsync();

            // Additional connectivity check (performed by AdminService)
            _adminService.ValidateKafkaConnectivity();


            // Log output: DLQ preparation complete
            Logger.LogInformation(
                "Kafka initialization completed; DLQ topic '{Topic}' ready with {Retention}ms retention",
                GetDlqTopicName(),
                _dslOptions.DlqOptions.RetentionMs);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                "FATAL: Kafka readiness check failed. DLQ functionality may be unavailable.", ex);
        }
    }
    public string GetDlqTopicName()
    {
        return _dslOptions.DlqTopicName;
    }
    /// <summary>
    /// Kafka connectivity validation.
    /// </summary>
    private void ValidateKafkaConnectivity()
    {
        try
        {
            // Kafka connectivity is verified during Producer/Consumer initialization.
            // No additional checks needed beyond the existing startup sequence.
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                "FATAL: Cannot connect to Kafka. Verify bootstrap servers and network connectivity.", ex);
        }
    }

    /// <summary>
    /// Register schemas for all entities and send dummy record if newly created
    /// </summary>
    private async Task RegisterSchemasAndMaterializeAsync()
    {
        var client = _schemaRegistryClient.Value;

        // Pass 1: Register schemas for all entities
        var entities = _entityModels.ToArray();
        var schemaResults = new Dictionary<Type, SchemaRegistrationResult>();
        foreach (var (type, model) in entities)
        {
            try
            {
                var mapping = _mappingRegistry.GetMapping(type);

                if (model.QueryModel == null && model.QueryExpression == null && model.HasKeys() && mapping.AvroKeySchema != null)
                {
                    var keySubject = $"{model.GetTopicName()}-key";
                    await client.RegisterSchemaIfNewAsync(keySubject, mapping.AvroKeySchema);
                    var keySchema = Avro.Schema.Parse(mapping.AvroKeySchema);
                    model.KeySchemaFullName = keySchema.Fullname;
                }

                var valueSubject = $"{model.GetTopicName()}-value";
                var valueResult = await client.RegisterSchemaIfNewAsync(valueSubject, mapping.AvroValueSchema!);
                var valueSchema = Avro.Schema.Parse(mapping.AvroValueSchema!);
                model.ValueSchemaFullName = valueSchema.Fullname;
                schemaResults[type] = valueResult;
                DecimalSchemaValidator.Validate(model, client, ValidationMode.Strict, Logger);
            }
            catch (ConfluentSchemaRegistry.SchemaRegistryException ex)
            {
                Logger.LogError(ex, "Schema registration failed for {Entity}", type.Name);
                throw;
            }
        }

        // Pass 2: Ensure DDL for simple entities first
        foreach (var (type, model) in entities.Where(e => e.Value.QueryModel == null && e.Value.QueryExpression == null))
        {
            await EnsureSimpleEntityDdlAsync(type, model);
        }

        // Pass 3: Then ensure DDL for query-defined entities
        foreach (var (type, model) in entities.Where(e => e.Value.QueryModel != null || e.Value.QueryExpression != null))
        {
            await EnsureQueryEntityDdlAsync(type, model);
        }

    }

    /// <summary>
    /// Create topics and ksqlDB objects for an entity defined without queries.
    /// </summary>
    private async Task EnsureSimpleEntityDdlAsync(Type type, EntityModel model)
    {


        var generator = new Kafka.Ksql.Linq.Query.Pipeline.DDLQueryGenerator();

        var topic = model.GetTopicName();
        if (_dslOptions.Topics.TryGetValue(topic, out var config) && config.Creation != null)
        {
            model.Partitions = config.Creation.NumPartitions;
            model.ReplicationFactor = config.Creation.ReplicationFactor;
        }
        // Fallback defaults for simple entity without explicit topic settings
        if (model.Partitions <= 0)
            model.Partitions = 1;
        if (model.ReplicationFactor <= 0)
            model.ReplicationFactor = 1;

        await _adminService.CreateDbTopicAsync(topic, model.Partitions, model.ReplicationFactor);

        // Wait for ksqlDB readiness briefly before issuing DDL
        await WaitForKsqlReadyAsync(TimeSpan.FromSeconds(15));

        string ddl;
        var schemaProvider = new Query.Ddl.EntityModelDdlAdapter(model);
        ddl = model.StreamTableType == StreamTableType.Table
            ? generator.GenerateCreateTable(schemaProvider)
            : generator.GenerateCreateStream(schemaProvider);
        Logger.LogInformation("KSQL DDL (simple {Entity}): {Sql}", type.Name, ddl);

        // Retry DDL with exponential backoff to tolerate ksqlDB command-topic warmup
        var attempts = 0;
        var maxAttempts = Math.Max(0, _dslOptions.KsqlDdlRetryCount) + 1; // include first try
        var delayMs = Math.Max(0, _dslOptions.KsqlDdlRetryInitialDelayMs);
        while (true)
        {
            var result = await ExecuteStatementAsync(ddl);
            if (result.IsSuccess)
                break;

            attempts++;
            var retryable = IsRetryableKsqlError(result.Message);
            if (!retryable || attempts >= maxAttempts)
            {
                var msg = $"DDL execution failed for {type.Name}: {result.Message}";
                Logger.LogError(msg);
                throw new InvalidOperationException(msg);
            }
            Logger.LogWarning("Retrying DDL for {Entity} (attempt {Attempt}/{Max}) due to: {Reason}", type.Name, attempts, maxAttempts - 1, result.Message);
            await _delay(TimeSpan.FromMilliseconds(delayMs), default);
            delayMs = Math.Min(delayMs * 2, 8000);
        }

        // Ensure the entity is visible to ksqlDB metadata before proceeding
        await WaitForEntityDdlAsync(model, TimeSpan.FromSeconds(12));
    }

    private async Task WaitForKsqlReadyAsync(TimeSpan timeout)
    {
        var start = DateTime.UtcNow;
        while (DateTime.UtcNow - start < timeout)
        {
            try
            {
                var ping = await ExecuteStatementAsync("SHOW TOPICS;");
                if (ping.IsSuccess)
                    return;
            }
            catch { }
            await Task.Delay(500);
        }
    }

    /// <summary>
    /// Ensure ksqlDB responds to a simple command within the timeout, or throw.
    /// Executes SHOW TOPICS in a loop, then performs one more SHOW TOPICS as a warmup.
    /// </summary>
    private async Task WarmupKsqlWithTopicsOrFail(TimeSpan timeout)
    {
        var start = DateTime.UtcNow;
        while (DateTime.UtcNow - start < timeout)
        {
            try
            {
                var res = await ExecuteStatementAsync("SHOW TOPICS;");
                if (res.IsSuccess)
                {
                    // one more warmup call (best-effort)
                    try { await ExecuteStatementAsync("SHOW TOPICS;"); } catch { }
                    return;
                }
            }
            catch { }
            await Task.Delay(500);
        }
        throw new TimeoutException("ksqlDB warmup timed out while waiting for SHOW TOPICS to succeed.");
    }

    private static bool IsRetryableKsqlError(string? message)
    {
        if (string.IsNullOrWhiteSpace(message)) return true;
        var m = message.ToLowerInvariant();
        // common transient indicators during startup/warmup
        return m.Contains("timeout while waiting for command topic")
            || m.Contains("could not write the statement")
            || m.Contains("failed to create new kafkaadminclient")
            || m.Contains("no resolvable bootstrap urls")
            || m.Contains("ksqldb server is not ready")
            || m.Contains("statement_error");
    }

    private static Type GetDerivedType(string name)
    {
        if (_derivedTypes.TryGetValue(name, out var t)) return t;
        var tb = _derivedModule.DefineType(name, System.Reflection.TypeAttributes.Public | System.Reflection.TypeAttributes.Class);
        t = tb.CreateType()!;
        _derivedTypes[name] = t;
        return t;
    }

    private async Task WaitForEntityDdlAsync(EntityModel model, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        var entity = model.GetTopicName().ToUpperInvariant();
        var stmt = $"DESCRIBE EXTENDED {entity};";

        while (DateTime.UtcNow < deadline)
        {
            try
            {
                var res = await ExecuteStatementAsync(stmt);
                if (res.IsSuccess && !string.IsNullOrWhiteSpace(res.Message))
                {
                    var msg = res.Message;
                    if (!msg.ToUpperInvariant().Contains("STATEMENT_ERROR") && HasDescribeInfo(msg))
                    {
                        await AssertTopicPartitionsAsync(model);
                        return;
                    }
                }
            }
            catch { }
            await Task.Delay(500);
        }

        static bool HasDescribeInfo(string message)
        {
            var lines = message.Split('\n');
            bool hasKey = lines.Any(l => l.Contains("Key format", StringComparison.OrdinalIgnoreCase));
            bool hasValue = lines.Any(l => l.Contains("Value format", StringComparison.OrdinalIgnoreCase));
            bool hasTs = lines.Any(l => l.Contains("Timestamp column", StringComparison.OrdinalIgnoreCase));
            bool hasStats = lines.Any(l => l.Contains("Runtime statistics", StringComparison.OrdinalIgnoreCase));
            bool hasSchema = lines.Any(l => l.Contains("|"));
            return hasKey && hasValue && hasTs && hasStats && hasSchema;
        }
    }

    private async Task AssertTopicPartitionsAsync(EntityModel model)
    {
        var res = await ExecuteStatementAsync("SHOW TOPICS;");
        if (!res.IsSuccess || string.IsNullOrWhiteSpace(res.Message))
            throw new InvalidOperationException("SHOW TOPICS failed");

        var lines = res.Message.Split('\n', StringSplitOptions.RemoveEmptyEntries);
        foreach (var line in lines)
        {
            if (!line.Contains('|')) continue;
            var parts = line.Split('|', StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length < 2) continue;
            if (parts[0].Trim().Equals(model.GetTopicName(), StringComparison.OrdinalIgnoreCase))
            {
                if (int.TryParse(parts[1].Trim(), out var partitions) && partitions == model.Partitions)
                    return;
                throw new InvalidOperationException($"Topic {model.GetTopicName()} partition mismatch");
            }
        }
        throw new InvalidOperationException($"Topic {model.GetTopicName()} not found");
    }

    private async Task EnsureQueryEntityDdlAsync(Type type, EntityModel model)
    {
        if (model.QueryModel != null)
            RegisterQueryModelMapping(model);

        if (model.QueryModel?.HasTumbling() == true)
        {
            var qao = BuildQao(model);
            await DerivedTumblingPipeline.RunAsync(
                qao,
                model,
                model.QueryModel,
                ExecuteWithRetryAsync,
                n => GetDerivedType(n),
                _mappingRegistry,
                _entityModels,
                Logger);

            // Ensure CTAS/CSAS queries for derived entities are RUNNING before proceeding
            await WaitForDerivedQueriesRunningAsync(TimeSpan.FromSeconds(60));
            return;
        }

        var isTable = model.GetExplicitStreamTableType() == StreamTableType.Table ||
            model.QueryModel?.DetermineType() == StreamTableType.Table;

        if (model.QueryModel?.DetermineType() == StreamTableType.Table)

        {
            Func<Type, string> resolver = t =>
            {
                var key = t?.Name ?? string.Empty;
                if (!string.IsNullOrEmpty(key) && _dslOptions.SourceNameOverrides is { Count: > 0 } && _dslOptions.SourceNameOverrides.TryGetValue(key, out var overrideName))
                    return overrideName;
                if (t != null && _entityModels.TryGetValue(t, out var srcModel))
                    return srcModel.GetTopicName().ToUpperInvariant();
                return key;
            };
            var ddl = Query.Builders.KsqlCreateStatementBuilder.Build(
                model.GetTopicName(),
                model.QueryModel,
                model.KeySchemaFullName,
                model.ValueSchemaFullName,
                resolver);
            Logger.LogInformation("KSQL DDL (query {Entity}): {Sql}", type.Name, ddl);
            await ExecuteWithRetryAsync(ddl);

            var qs = await ExecuteStatementAsync("SHOW QUERIES;");
            if (!qs.IsSuccess || string.IsNullOrWhiteSpace(qs.Message) || !QueryRunning(qs.Message))
                throw new InvalidOperationException($"CTAS query for {model.GetTopicName()} not running");

            await AssertTopicPartitionsAsync(model);
            return;

            bool QueryRunning(string message)
            {
                var lines = message.Split('\n', StringSplitOptions.RemoveEmptyEntries);
                foreach (var line in lines)
                {
                    if (!line.Contains('|')) continue;
                    var parts = line.Split('|', StringSplitOptions.RemoveEmptyEntries);
                    if (parts.Length > 3 &&
                        parts[1].Trim().Equals(model.GetTopicName(), StringComparison.OrdinalIgnoreCase) &&
                        parts[3].Trim().Equals("RUNNING", StringComparison.OrdinalIgnoreCase))
                        return true;
                }
                return false;
            }
        }

        var generator = new Kafka.Ksql.Linq.Query.Pipeline.DDLQueryGenerator();
        var adapter = new EntityModelDdlAdapter(model);
        var ddlSql = isTable
            ? generator.GenerateCreateTable(adapter)
            : generator.GenerateCreateStream(adapter);
        Logger.LogInformation("KSQL DDL (query {Entity}): {Sql}", type.Name, ddlSql);
        await ExecuteWithRetryAsync(ddlSql);

        if (model.QueryModel != null)
        {
            Func<Type, string> resolver = t =>
            {
                var key = t?.Name ?? string.Empty;
                if (!string.IsNullOrEmpty(key) && _dslOptions.SourceNameOverrides is { Count: > 0 } && _dslOptions.SourceNameOverrides.TryGetValue(key, out var overrideName))
                    return overrideName;
                if (t != null && _entityModels.TryGetValue(t, out var srcModel))
                    return srcModel.GetTopicName().ToUpperInvariant();
                return key;
            };
            var insert = Query.Builders.KsqlInsertStatementBuilder.Build(model.GetTopicName(), model.QueryModel, resolver);
            Logger.LogInformation("KSQL DDL (query {Entity}): {Sql}", type.Name, insert);
            await ExecuteWithRetryAsync(insert);
        }

        async Task ExecuteWithRetryAsync(string sql)
        {
            var attempts = 0;
            var maxAttempts = Math.Max(0, _dslOptions.KsqlDdlRetryCount) + 1;
            var delayMs = Math.Max(0, _dslOptions.KsqlDdlRetryInitialDelayMs);
            while (true)
            {
                var result = await ExecuteStatementAsync(sql);
                if (result.IsSuccess)
                    break;
                // Treat idempotent CREATE conflicts as success to avoid flakiness across runs
                if (IsNonFatalCreateConflict(sql, result.Message))
                    break;
                attempts++;
                var retryable = IsRetryableKsqlError(result.Message);
                if (!retryable || attempts >= maxAttempts)
                {
                    var msg = $"DDL execution failed for {type.Name}: {result.Message}";
                    Logger.LogError(msg);
                    throw new InvalidOperationException(msg);
                }
                Logger.LogWarning("Retrying DDL for {Entity} (attempt {Attempt}/{Max}) due to: {Reason}", type.Name, attempts, maxAttempts - 1, result.Message);
                await _delay(TimeSpan.FromMilliseconds(delayMs), default);
                delayMs = Math.Min(delayMs * 2, 8000);
            }
        }

        static bool IsNonFatalCreateConflict(string sql, string? message)
        {
            if (string.IsNullOrWhiteSpace(sql) || string.IsNullOrWhiteSpace(message)) return false;
            var s = sql.TrimStart().ToUpperInvariant();
            if (!s.StartsWith("CREATE ")) return false;
            var m = message.ToLowerInvariant();
            return m.Contains("already exists");
        }

        TumblingQao BuildQao(EntityModel m)
        {
            var ctx = new NullabilityInfoContext();
            var shape = m.AllProperties.Select(p => new ColumnShape(p.Name, p.PropertyType, ctx.Create(p).WriteState == NullabilityState.Nullable)).ToArray();
            var frames = m.QueryModel!.Windows.Select(ParseWindow).ToList();
            var keys = m.KeyProperties.Select(p => p.Name).ToArray();
            var basedOnKeys = m.QueryModel.BasedOnJoinKeys.Count > 0 ? m.QueryModel.BasedOnJoinKeys.ToArray() : keys;
            var dayKey = PropertyName(m.QueryModel.BasedOnDayKey);
            var timeKey = m.QueryModel!.TimeKey
                ?? m.AllProperties.FirstOrDefault(p => p.GetCustomAttribute<KsqlTimestampAttribute>() != null)?.Name
                ?? string.Empty;
            var projection = m.AllProperties.Select(p => p.Name).Where(n => !keys.Contains(n)).ToArray();
            var basedOnOpen = m.QueryModel.BasedOnOpen ?? string.Empty;
            var basedOnClose = m.QueryModel.BasedOnClose ?? string.Empty;
            return new TumblingQao
            {
                TimeKey = timeKey,
                Windows = frames,
                Keys = keys,
                Projection = projection,
                PocoShape = shape,
                BasedOn = new BasedOnSpec(
                    basedOnKeys,
                    basedOnOpen,
                    basedOnClose,
                    dayKey,
                    m.QueryModel.BasedOnOpenInclusive,
                    m.QueryModel.BasedOnCloseInclusive),
                WeekAnchor = m.QueryModel!.WeekAnchor,
                BaseUnitSeconds = m.QueryModel!.BaseUnitSeconds,
                GraceSeconds = m.QueryModel!.GraceSeconds
            };
        }

        Timeframe ParseWindow(string w)
        {
            if (w.EndsWith("mo", StringComparison.OrdinalIgnoreCase))
                return new Timeframe(int.Parse(w[..^2]), "mo");
            if (w.EndsWith("wk", StringComparison.OrdinalIgnoreCase))
                return new Timeframe(int.Parse(w[..^2]), "wk");
            var unit = w[^1].ToString();
            var value = int.Parse(w[..^1]);
            return new Timeframe(value, unit);
        }

        static string PropertyName(LambdaExpression? e) =>
            e?.Body is MemberExpression m ? m.Member.Name : string.Empty;
    }

    private async Task WaitForDerivedQueriesRunningAsync(TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        // Collect derived entity names (those with timeframe/role markers)
        var targets = _entityModels.Values
            .Where(m => m.AdditionalSettings.ContainsKey("timeframe") && m.AdditionalSettings.ContainsKey("role"))
            .Select(m => m.GetTopicName().ToUpperInvariant())
            .Distinct()
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
        if (targets.Count == 0) return;

        while (DateTime.UtcNow < deadline)
        {
            try
            {
                var qs = await ExecuteStatementAsync("SHOW QUERIES;");
                if (qs.IsSuccess && !string.IsNullOrWhiteSpace(qs.Message))
                {
                    var running = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    var lines = qs.Message.Split('\n', StringSplitOptions.RemoveEmptyEntries);
                    foreach (var line in lines)
                    {
                        if (!line.Contains('|')) continue;
                        var parts = line.Split('|', StringSplitOptions.RemoveEmptyEntries);
                        if (parts.Length > 3)
                        {
                            var entity = parts[1].Trim();
                            var status = parts[3].Trim();
                            if (status.Equals("RUNNING", StringComparison.OrdinalIgnoreCase))
                                running.Add(entity);
                        }
                    }
                    if (targets.All(t => running.Contains(t))) return;
                }
            }
            catch { }
            await Task.Delay(500);
        }
        // Best-effort; continue even if not all queries reported RUNNING to avoid hard hangs in CI
    }

    /// <summary>
    /// Register mapping information for a query-defined entity using its KsqlQueryModel.
    /// </summary>
    private void RegisterQueryModelMapping(EntityModel model)
    {
        if (model.QueryModel == null)
            return;

        var isTable = model.GetExplicitStreamTableType() == StreamTableType.Table ||
            model.QueryModel!.DetermineType() == StreamTableType.Table;
        _mappingRegistry.RegisterQueryModel(
            model.EntityType,
            model.QueryModel!,
            model.KeyProperties,
            model.GetTopicName(),
            genericValue: isTable);
    }


    private static object CreateDummyInstance(Type entityType)
    {
        var method = typeof(Application.DummyObjectFactory).GetMethod("CreateDummy")!
            .MakeGenericMethod(entityType);
        return method.Invoke(null, null)!;
    }
}
