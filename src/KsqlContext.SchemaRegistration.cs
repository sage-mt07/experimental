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
using System.Threading;
using System.Linq.Expressions;
using System.Text;
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
            await EnsureDerivedQueryEntityDdlAsync(type, model);
            return;
        }
        var isTable = model.GetExplicitStreamTableType() == StreamTableType.Table ||
            model.QueryModel?.DetermineType() == StreamTableType.Table;
        if (model.QueryModel?.DetermineType() == StreamTableType.Table)
        {
            await EnsureTableQueryEntityDdlAsync(type, model);
            return;
        }
        var generator = new Kafka.Ksql.Linq.Query.Pipeline.DDLQueryGenerator();
        var adapter = new EntityModelDdlAdapter(model);
        var ddlSql = isTable
            ? generator.GenerateCreateTable(adapter)
            : generator.GenerateCreateStream(adapter);
        Logger.LogInformation("KSQL DDL (query {Entity}): {Sql}", type.Name, ddlSql);
        _ = await ExecuteWithRetryAsync(ddlSql);
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
            _ = await ExecuteWithRetryAsync(insert);
        }
        return;
        async Task EnsureDerivedQueryEntityDdlAsync(Type entityType, EntityModel baseModel)
        {
            var attempts = GetPersistentQueryMaxAttempts();
            var delay = TimeSpan.FromSeconds(5);
            Exception? lastError = null;
            for (var attempt = 0; attempt < attempts; attempt++)
            {
                var results = await DerivedTumblingPipeline.RunAsync(
                    BuildQao(baseModel),
                    baseModel,
                    baseModel.QueryModel!,
                    ExecuteDerivedAsync,
                    n => GetDerivedType(n),
                    _mappingRegistry,
                    _entityModels,
                    Logger);
                var persistent = await CollectPersistentExecutionsAsync(results).ConfigureAwait(false);
                try
                {
                    if (persistent.Count > 0)
                        await StabilizePersistentQueriesAsync(persistent, baseModel, GetPersistentQueryTimeout(), default);
                    await WaitForDerivedQueriesRunningAsync(GetQueryRunningTimeout());
                    return;
                }
                catch (Exception ex) when (attempt + 1 < attempts)
                {
                    lastError = ex;
                    Logger.LogWarning(ex, "Derived query stabilization failed for {Entity} (attempt {Attempt}/{Max})", entityType.Name, attempt + 1, attempts);
                    await TerminateQueriesAsync(persistent);
                    await Task.Delay(delay);
                }
                catch (Exception ex)
                {
                    lastError = ex;
                    break;
                }
            }
            throw lastError ?? new TimeoutException($"Derived query for {entityType.Name} did not stabilize.");
            Task<KsqlDbResponse> ExecuteDerivedAsync(EntityModel m, string sql)
                => ExecuteWithRetryAsync(sql);
            async Task<List<PersistentQueryExecution>> CollectPersistentExecutionsAsync(IReadOnlyList<DerivedTumblingPipeline.ExecutionResult> executions)
            {
                var list = new List<PersistentQueryExecution>();
                foreach (var execution in executions)
                {
                    if (!execution.IsPersistentQuery)
                        continue;
                    var topicName = execution.Model.GetTopicName();
                    var queryId = await TryGetQueryIdFromShowQueriesAsync(topicName, execution.Statement).ConfigureAwait(false);
                    if (!string.IsNullOrEmpty(queryId))
                    {
                        list.Add(new PersistentQueryExecution(
                            queryId,
                            execution.Model,
                            topicName,
                            execution.Statement,
                            execution.InputTopic,
                            true));
                    }
                    else
                    {
                        Logger.LogWarning("Could not locate queryId via SHOW QUERIES for derived statement targeting {Topic}: {Statement}", topicName, execution.Statement);
                    }
                }
                return list;
            }
        }
        async Task EnsureTableQueryEntityDdlAsync(Type entityType, EntityModel tableModel)
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
                tableModel.GetTopicName(),
                tableModel.QueryModel!,
                tableModel.KeySchemaFullName,
                tableModel.ValueSchemaFullName,
                resolver);
            Logger.LogInformation("KSQL DDL (query {Entity}): {Sql}", entityType.Name, ddl);
            var attempts = GetPersistentQueryMaxAttempts();
            var delay = TimeSpan.FromSeconds(5);
            Exception? lastError = null;
            for (var attempt = 0; attempt < attempts; attempt++)
            {
                _ = await ExecuteWithRetryAsync(ddl);
                var persistent = await CollectPersistentExecutionsAsync().ConfigureAwait(false);
                try
                {
                    if (persistent.Count > 0)
                        await StabilizePersistentQueriesAsync(persistent, tableModel, GetPersistentQueryTimeout(), default);
                    await WaitForQueryRunningAsync(tableModel.GetTopicName(), TimeSpan.FromSeconds(60));
                    await AssertTopicPartitionsAsync(tableModel);
                    return;
                }
                catch (Exception ex) when (attempt + 1 < attempts)
                {
                    lastError = ex;
                    Logger.LogWarning(ex, "Persistent query stabilization failed for {Entity} (attempt {Attempt}/{Max})", entityType.Name, attempt + 1, attempts);
                    await TerminateQueriesAsync(persistent);
                    await Task.Delay(delay);
                }
                catch (Exception ex)
                {
                    lastError = ex;
                    break;
                }
            }
            throw lastError ?? new TimeoutException($"Persistent query for {entityType.Name} did not stabilize.");
            async Task<List<PersistentQueryExecution>> CollectPersistentExecutionsAsync()
            {
                var list = new List<PersistentQueryExecution>();
                var topicName = tableModel.GetTopicName();
                var queryId = await TryGetQueryIdFromShowQueriesAsync(topicName, ddl).ConfigureAwait(false);
                if (!string.IsNullOrEmpty(queryId))
                {
                    list.Add(new PersistentQueryExecution(
                        queryId,
                        tableModel,
                        topicName,
                        ddl,
                        null,
                        false));
                }
                else if (ddl.IndexOf(" AS ", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    Logger.LogWarning("Could not locate queryId via SHOW QUERIES for CTAS statement executed for {Entity}", entityType.Name);
                }
                return list;
            }
        }
        async Task<KsqlDbResponse> ExecuteWithRetryAsync(string sql)
        {
            var attempts = 0;
            var maxAttempts = Math.Max(0, _dslOptions.KsqlDdlRetryCount) + 1;
            var delayMs = Math.Max(0, _dslOptions.KsqlDdlRetryInitialDelayMs);
            while (true)
            {
                var result = await ExecuteStatementAsync(sql);
                if (result.IsSuccess)
                    return result;
                // Treat idempotent CREATE conflicts as success to avoid flakiness across runs
                if (IsNonFatalCreateConflict(sql, result.Message))
                    return new KsqlDbResponse(true, result.Message ?? string.Empty, result.ErrorCode, result.ErrorDetail);
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
            throw new InvalidOperationException("Unexpected DDL retry flow exited without result");
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
        // Collect derived entity names (those with timeframe/role markers)
        var targets = _entityModels.Values
            .Where(m => m.AdditionalSettings.ContainsKey("timeframe") && m.AdditionalSettings.ContainsKey("role"))
            .Select(m => m.GetTopicName())
            .Distinct()
            .ToList();
        foreach (var t in targets)
            await WaitForQueryRunningAsync(t, timeout);
    }
    private async Task WaitForQueryRunningAsync(string targetEntityName, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        var target = targetEntityName.ToUpperInvariant();
        var required = GetRequiredConsecutiveSuccess();
        var stability = GetStabilityWindow();
        var consecutive = 0;
        while (DateTime.UtcNow < deadline)
        {
            var qs = await ExecuteStatementAsync("SHOW QUERIES;");
            if (qs.IsSuccess && !string.IsNullOrWhiteSpace(qs.Message))
            {
                var lines = qs.Message.Split('\n', StringSplitOptions.RemoveEmptyEntries);
                bool isRunning = false;
                foreach (var line in lines)
                {
                    if (!line.Contains('|')) continue;
                    var parts = line.Split('|', StringSplitOptions.RemoveEmptyEntries);
                    // Robust match: if the line contains the target topic and RUNNING state anywhere
                    var lineUpper = line.ToUpperInvariant();
                    if (lineUpper.Contains(target) && lineUpper.Contains("RUNNING"))
                    {
                        isRunning = true;
                        break;
                    }
                    // Fallback to column-based match when available
                    if (parts.Length > 3 && parts[1].Trim().Equals(target, StringComparison.OrdinalIgnoreCase))
                    {
                        var state = parts[3].Trim().ToUpperInvariant();
                        if (state == "RUNNING") isRunning = true; else isRunning = false;
                        break;
                    }
                }
                if (isRunning)
                {
                    consecutive++;
                    if (consecutive >= required)
                    {
                        // optional stability window
                        if (stability > TimeSpan.Zero)
                        {
                            await Task.Delay(stability);
                            // re-check once more
                            var confirm = await ExecuteStatementAsync("SHOW QUERIES;");
                            if (confirm.IsSuccess && !string.IsNullOrWhiteSpace(confirm.Message))
                            {
                                var confirmLines = confirm.Message.Split('\n', StringSplitOptions.RemoveEmptyEntries);
                                foreach (var line in confirmLines)
                                {
                                    if (!line.Contains('|')) continue;
                                    var parts = line.Split('|', StringSplitOptions.RemoveEmptyEntries);
                                    var lineUpper = line.ToUpperInvariant();
                                    if (lineUpper.Contains(target) && lineUpper.Contains("RUNNING")) { return; }
                                    if (parts.Length > 3 && parts[1].Trim().Equals(target, StringComparison.OrdinalIgnoreCase))
                                    {
                                        var state = parts[3].Trim().ToUpperInvariant();
                                        if (state == "RUNNING") return;
                                        break;
                                    }
                                }
                                // If fell out of RUNNING, reset and continue
                                consecutive = 0;
                            }
                        }
                        else
                        {
                            return;
                        }
                    }
                }
                else
                {
                    consecutive = 0;
                }
            }
            await Task.Delay(2000);
        }
        throw new TimeoutException($"CTAS/CSAS query for {targetEntityName} did not reach RUNNING within {timeout.TotalSeconds}s");
    }
    private static TimeSpan GetQueryRunningTimeout()
    {
        var env = Environment.GetEnvironmentVariable("KSQL_QUERY_RUNNING_TIMEOUT_SECONDS");
        if (int.TryParse(env, out var seconds) && seconds > 0)
            return TimeSpan.FromSeconds(seconds);
        return TimeSpan.FromSeconds(180);
    }
    private static int GetRequiredConsecutiveSuccess()
    {
        var env = Environment.GetEnvironmentVariable("KSQL_QUERY_RUNNING_CONSECUTIVE");
        if (int.TryParse(env, out var n) && n > 0) return n;
        return 5; // default per physical stability guidance
    }
    private static TimeSpan GetStabilityWindow()
    {
        var env = Environment.GetEnvironmentVariable("KSQL_QUERY_RUNNING_STABILITY_WINDOW_SECONDS");
        if (int.TryParse(env, out var seconds) && seconds >= 0)
            return TimeSpan.FromSeconds(seconds);
        return TimeSpan.FromSeconds(15);
    }
    private async Task StabilizePersistentQueriesAsync(
        IReadOnlyList<PersistentQueryExecution> executions,
        EntityModel? baseModel,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        if (_adminService == null || executions == null || executions.Count == 0)
        {
            Logger?.LogWarning("KafkaAdminService unavailable; skipping persistent query stabilization.");
            return;
        }
        foreach (var execution in executions)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var parents = GetParentTopicCandidates(execution, baseModel);
            var partitions = ResolveParentPartitions(parents, execution.TargetModel);
            await EnsureInternalTopicsReadyAsync(execution, partitions, timeout, cancellationToken).ConfigureAwait(false);
        }
    }
    private async Task TerminateQueriesAsync(IReadOnlyList<PersistentQueryExecution> executions)
    {
        if (executions == null || executions.Count == 0)
            return;
        foreach (var execution in executions)
        {
            try
            {
                var terminate = $"TERMINATE {execution.QueryId};";
                var response = await ExecuteStatementAsync(terminate).ConfigureAwait(false);
                if (!response.IsSuccess)
                {
                    Logger.LogWarning("Termination of query {QueryId} reported non-success: {Message}", execution.QueryId, response.Message);
                }
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Failed to terminate query {QueryId}", execution.QueryId);
            }
        }
    }
    private IEnumerable<string> GetParentTopicCandidates(PersistentQueryExecution execution, EntityModel? baseModel)
    {
        var topics = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (!string.IsNullOrWhiteSpace(execution.InputTopic))
            topics.Add(execution.InputTopic!);
        var target = execution.TargetModel;
        if (target.QueryModel != null)
        {
            var sourceTypes = target.QueryModel.SourceTypes ?? Array.Empty<Type>();
            foreach (var source in sourceTypes)
            {
                if (_entityModels.TryGetValue(source, out var srcModel))
                {
                    topics.Add(srcModel.GetTopicName());
                    continue;
                }
                if (_dslOptions.SourceNameOverrides is { Count: > 0 } && _dslOptions.SourceNameOverrides.TryGetValue(source.Name, out var overrideName) && !string.IsNullOrWhiteSpace(overrideName))
                {
                    topics.Add(overrideName);
                    continue;
                }
                topics.Add(source.Name.ToUpperInvariant());
            }
        }
        else if (baseModel != null)
        {
            topics.Add(baseModel.GetTopicName());
        }
        return topics;
    }
        private int ResolveParentPartitions(IEnumerable<string> parentTopics, EntityModel fallbackModel)
    {
        foreach (var topic in parentTopics)
        {
            var candidate = topic?.Trim();
            if (string.IsNullOrWhiteSpace(candidate))
                continue;
            var metadata = _adminService.TryGetTopicMetadata(candidate);
            metadata ??= _adminService.TryGetTopicMetadata(candidate.ToLowerInvariant());
            if (metadata?.Partitions != null && metadata.Partitions.Count > 0)
                return metadata.Partitions.Count;
        }
        if (fallbackModel.Partitions > 0)
            return fallbackModel.Partitions;
        return 1;
    }
    private async Task EnsureInternalTopicsReadyAsync(PersistentQueryExecution execution, int partitions, TimeSpan timeout, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(execution.QueryId))
            return;
        if (partitions <= 0)
            partitions = 1;
        var (repartitionTopic, changelogTopic) = BuildInternalTopicNames(execution.QueryId);
        await EnsureInternalTopicReadyAsync(repartitionTopic, partitions, timeout, cancellationToken).ConfigureAwait(false);
        await EnsureInternalTopicReadyAsync(changelogTopic, partitions, timeout, cancellationToken).ConfigureAwait(false);
    }
    private async Task EnsureInternalTopicReadyAsync(string topicName, int partitions, TimeSpan timeout, CancellationToken cancellationToken)
    {
        var deadline = DateTime.UtcNow + timeout;
        var pollDelay = TimeSpan.FromSeconds(2);
        var createDelay = TimeSpan.FromSeconds(6);
        var creationThreshold = DateTime.UtcNow + createDelay;
        var creationAttempted = false;
        while (DateTime.UtcNow < deadline)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var metadata = _adminService.TryGetTopicMetadata(topicName) ?? _adminService.TryGetTopicMetadata(topicName.ToLowerInvariant());
            if (metadata?.Partitions != null && metadata.Partitions.Count > 0)
            {
                if (metadata.Partitions.Count != partitions)
                {
                    Logger?.LogWarning("Internal topic {Topic} has {Actual} partitions, expected {Expected}", topicName, metadata.Partitions.Count, partitions);
                }
                return;
            }
            if (!creationAttempted && DateTime.UtcNow >= creationThreshold)
            {
            try
            {
                Logger?.LogInformation("Creating internal topic {Topic} with {Partitions} partitions (manual)", topicName, partitions);
                await _adminService.CreateDbTopicAsync(topicName, partitions, 1).ConfigureAwait(false);
                Logger?.LogInformation("Internal topic {Topic} created successfully", topicName);
            }
            catch (Exception ex)
            {
                Logger?.LogWarning(ex, "Failed to create internal topic {Topic}", topicName);
            }
            creationAttempted = true;
        }
            await Task.Delay(pollDelay, cancellationToken).ConfigureAwait(false);
        }
        throw new TimeoutException($"Internal topic {topicName} was not ready within {timeout.TotalSeconds:F0}s");
    }
    private (string RepartitionTopic, string ChangelogTopic) BuildInternalTopicNames(string queryId)
    {
        var serviceId = _dslOptions.KsqlServer?.ServiceId;
        if (string.IsNullOrWhiteSpace(serviceId))
            serviceId = "ksql_service_1";
        var prefix = _dslOptions.KsqlServer?.PersistentQueryPrefix;
        if (string.IsNullOrWhiteSpace(prefix))
            prefix = "query_";
        var baseName = $"_confluent-ksql-{serviceId}{prefix}{queryId}-Aggregate-";
        return ($"{baseName}GroupBy-repartition", $"{baseName}Aggregate-Materialize-changelog");
    }
    private async Task<string?> TryGetQueryIdFromShowQueriesAsync(string targetTopic, string? statement, int attempts = 5, int delayMs = 1000)
    {
        for (var attempt = 0; attempt < attempts; attempt++)
        {
            var response = await ExecuteStatementAsync("SHOW QUERIES;").ConfigureAwait(false);
            if (response.IsSuccess && !string.IsNullOrWhiteSpace(response.Message))
            {
                Logger?.LogInformation("SHOW QUERIES output (attempt {Attempt}): {Output}", attempt + 1, response.Message);
                var queryId = FindQueryIdInShowQueries(response.Message, targetTopic, statement);
                if (!string.IsNullOrEmpty(queryId))
                    return queryId;
                Logger?.LogWarning("SHOW QUERIES attempt {Attempt} did not contain a matching query for {Topic}", attempt + 1, targetTopic);
            }
            else
            {
                Logger?.LogWarning("SHOW QUERIES attempt {Attempt} failed or returned empty message", attempt + 1);
            }

            await _delay(TimeSpan.FromMilliseconds(delayMs), default).ConfigureAwait(false);
        }

        Logger?.LogWarning("Unable to locate queryId for {Topic} after {Attempts} attempts", targetTopic, attempts);
        return null;
    }

    internal static string? FindQueryIdInShowQueries(string showQueriesOutput, string targetTopic, string? statement)
    {
        if (string.IsNullOrWhiteSpace(showQueriesOutput))
            return null;

        var normalizedTargetTopic = NormalizeIdentifierForMatch(targetTopic);
        var normalizedStatement = NormalizeSqlForMatch(statement);

        var lines = showQueriesOutput.Split('\n', StringSplitOptions.RemoveEmptyEntries);
        foreach (var rawLine in lines)
        {
            var line = rawLine.Trim();
            if (string.IsNullOrEmpty(line) || line.StartsWith("+") || line.StartsWith("-"))
                continue;

            if (!line.Contains('|'))
                continue;

            var lineUpper = line.ToUpperInvariant();

            var columns = ParseColumns(line);

            if (columns.Count == 0)
                continue;

            var queryId = columns[0];
            if (string.IsNullOrEmpty(queryId) || queryId.Equals("Query ID", StringComparison.OrdinalIgnoreCase))
                continue;

            var topicMatches = false;
            if (!string.IsNullOrEmpty(normalizedTargetTopic))
            {
                if (columns.Count > 1)
                {
                    var topicColumn = columns[1];
                    var topics = topicColumn.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                        .Select(t => NormalizeIdentifierForMatch(t.Replace("\"", string.Empty)))
                        .ToList();
                    topicMatches = topics.Any(t => !string.IsNullOrEmpty(t) && t.Equals(normalizedTargetTopic, StringComparison.OrdinalIgnoreCase));
                }

                if (!topicMatches)
                    topicMatches = lineUpper.Contains(normalizedTargetTopic);
            }

            var statementMatches = false;
            if (!string.IsNullOrEmpty(normalizedStatement))
            {
                var normalizedColumnStatement = columns.Count > 2 ? NormalizeSqlForMatch(columns[2]) : string.Empty;
                statementMatches = lineUpper.Contains(normalizedStatement) ||
                    (!string.IsNullOrEmpty(normalizedColumnStatement) && normalizedColumnStatement.Contains(normalizedStatement, StringComparison.Ordinal));
            }

            if (topicMatches || statementMatches)
                return queryId;
        }

        return null;
    }

    private static string NormalizeIdentifierForMatch(string? value)
    {
        return string.IsNullOrWhiteSpace(value)
            ? string.Empty
            : value.Trim().Trim('\"').ToUpperInvariant();
    }

    private static string NormalizeSqlForMatch(string? sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
            return string.Empty;

        var builder = new System.Text.StringBuilder(sql.Length);
        var previousWasWhitespace = false;
        foreach (var ch in sql)
        {
            if (char.IsWhiteSpace(ch))
            {
                if (!previousWasWhitespace)
                {
                    builder.Append(' ');
                    previousWasWhitespace = true;
                }
            }
            else
            {
                builder.Append(char.ToUpperInvariant(ch));
                previousWasWhitespace = false;
            }
        }

        return builder.ToString().Trim();
    }

    private static List<string> ParseColumns(string line)
    {
        var list = new List<string>();
        if (string.IsNullOrEmpty(line))
            return list;

        var builder = new System.Text.StringBuilder();
        foreach (var ch in line)
        {
            if (ch == '|')
            {
                AddColumn(builder, list);
            }
            else
            {
                builder.Append(ch);
            }
        }
        AddColumn(builder, list);
        return list;

        static void AddColumn(System.Text.StringBuilder sb, List<string> target)
        {
            if (sb.Length == 0)
                return;
            var value = sb.ToString().Trim();
            sb.Clear();
            if (value.Length > 0)
                target.Add(value);
        }
    }
    private static int GetPersistentQueryMaxAttempts()
    {
        var env = Environment.GetEnvironmentVariable("KSQL_PERSISTENT_QUERY_MAX_ATTEMPTS");
        if (int.TryParse(env, out var attempts) && attempts > 0)
            return attempts;
        return 3;
    }
    private static TimeSpan GetPersistentQueryTimeout()
    {
        var env = Environment.GetEnvironmentVariable("KSQL_PERSISTENT_QUERY_READY_TIMEOUT_SECONDS");
        if (int.TryParse(env, out var seconds) && seconds > 0)
            return TimeSpan.FromSeconds(seconds);
        return TimeSpan.FromSeconds(45);
    }
    private sealed record PersistentQueryExecution(
        string QueryId,
        EntityModel TargetModel,
        string TargetTopic,
        string Statement,
        string? InputTopic,
        bool IsDerived);
    /// <summary>
    /// Register mapping information for a query-defined entity using its KsqlQueryModel.
    /// </summary>
    private void RegisterQueryModelMapping(EntityModel model)
    {
        if (model.QueryModel == null)
            return;
        var derivedType = model.QueryModel!.DetermineType();
        var isTable = model.GetExplicitStreamTableType() == StreamTableType.Table ||
            derivedType == StreamTableType.Table;
        // Ensure query-defined entities get the correct Stream/Table classification
        // so downstream components (e.g., cache enabling/resolution) behave correctly.
        model.SetStreamTableType(isTable ? StreamTableType.Table : StreamTableType.Stream);
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
