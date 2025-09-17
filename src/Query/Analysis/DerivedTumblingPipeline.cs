using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Query.Adapters;
using Kafka.Ksql.Linq.Query.Builders;
using Kafka.Ksql.Linq.Query.Builders.Core;
using Kafka.Ksql.Linq.Query.Dsl;
using Kafka.Ksql.Linq.Query.Abstractions;
using Kafka.Ksql.Linq.Mapping;
using Kafka.Ksql.Linq;
using Kafka.Ksql.Linq.Core.Extensions;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Kafka.Ksql.Linq.Query.Analysis;



internal static class DerivedTumblingPipeline

{

    public sealed record ExecutionResult(

        EntityModel Model,

        Role Role,

        string Statement,

        string? InputTopic,

        KsqlDbResponse Response)

    {

        public string TargetTopic => Model.GetTopicName();

        public bool IsPersistentQuery => Statement.IndexOf(" AS ", StringComparison.OrdinalIgnoreCase) >= 0;

    }



    public static async Task<IReadOnlyList<ExecutionResult>> RunAsync(

        TumblingQao qao,

        EntityModel baseModel,

        KsqlQueryModel queryModel,

        Func<EntityModel, string, Task<KsqlDbResponse>> execute,

        Func<string, Type> resolveType,

        MappingRegistry mapping,

        ConcurrentDictionary<Type, EntityModel> registry,

        ILogger logger)

    {

        var executions = new List<ExecutionResult>();

        var baseAttr = baseModel.EntityType.GetCustomAttribute<KsqlTopicAttribute>();

        var baseName = (baseAttr?.Name ?? baseModel.TopicName ?? baseModel.EntityType.Name).ToLowerInvariant();

        var entities = PlanDerivedEntities(qao, baseModel, queryModel.WhenEmptyFiller != null);

        var models = AdaptModels(entities);

        // Ensure deterministic and dependency-safe ordering:

        // - Create 1s TABLE before the hub STREAM to ensure topic exists

        // - Then other roles ordered by role priority and timeframe ascending

        static int RolePriority(Role role) => role switch

        {

            Role.Final1s => 0,

            Role.Final1sStream => 1,

            Role.Hb => 2,

            Role.Prev1m => 3,

            Role.Live => 4,

            Role.Fill => 5,

            Role.Final => 6,

            _ => 9

        };

        static int TimeframeToSeconds(string tf)

        {

            if (string.IsNullOrWhiteSpace(tf)) return int.MaxValue;

            if (tf.EndsWith("mo", StringComparison.OrdinalIgnoreCase)) return 30 * 24 * 60 * 60 * int.Parse(tf[..^2]);

            if (tf.EndsWith("wk", StringComparison.OrdinalIgnoreCase)) return 7 * 24 * 60 * 60 * int.Parse(tf[..^2]);

            var unit = char.ToLowerInvariant(tf[^1]);

            if (!int.TryParse(tf[..^1], out var v)) v = 0;

            return unit switch

            {

                's' => v,

                'm' => v * 60,

                'h' => v * 3600,

                'd' => v * 86400,

                _ => int.MaxValue

            };

        }

        var pass1 = models.Where(m => (m.AdditionalSettings.TryGetValue("id", out var id1) ? id1?.ToString() : string.Empty)?.EndsWith("_1s_final", StringComparison.OrdinalIgnoreCase) == true).ToList();

        var pass2 = models.Where(m => (m.AdditionalSettings.TryGetValue("id", out var id2) ? id2?.ToString() : string.Empty)?.EndsWith("_1s_final_s", StringComparison.OrdinalIgnoreCase) == true).ToList();

        var rest = models.Except(pass1).Except(pass2)

            .Select(m => (Model: m, Role: Enum.Parse<Role>((string)m.AdditionalSettings["role"]), Tf: (string)m.AdditionalSettings["timeframe"]))

            .OrderBy(x => RolePriority(x.Role))

            .ThenBy(x => TimeframeToSeconds(x.Tf))

            .Select(x => x.Model)

            .ToList();

        var executionOrder = pass1.Concat(pass2).Concat(rest);

        foreach (var m in executionOrder)

        {

            var role = Enum.Parse<Role>((string)m.AdditionalSettings["role"]);

            var tf = (string)m.AdditionalSettings["timeframe"];

            var allow = role switch

            {

                Role.Final1sStream or Role.Final1s => tf == "1s",

                Role.Prev1m => tf == "1m",

                Role.Live => true,

                Role.Hb => true,

                Role.Fill => true,

                _ => true

            };

            if (!allow)

                continue;

            var (ddl, dt, ns, inputOverride) = BuildDdlAndRegister(baseName, queryModel, m, role, resolveType);



            // Register TimeBucket read mapping so TimeBucket<T> resolves to the

            // concrete per-timeframe entity type instead of the base class.

            try

            {

                Kafka.Ksql.Linq.Runtime.Period period = ParsePeriod(tf);

                Kafka.Ksql.Linq.Runtime.TimeBucketTypes.RegisterRead(baseModel.EntityType, period, dt);

            }

            catch { /* best-effort; do not block DDL */ }

            logger.LogInformation("KSQL DDL (derived {Entity}): {Sql}", m.TopicName, ddl);

            var response = await execute(m, ddl);

            executions.Add(new ExecutionResult(m, role, ddl, inputOverride, response));

            // Register mapping using explicit shapes captured in AdditionalSettings

            try

            {

                var keyNames = m.AdditionalSettings.TryGetValue("keys", out var kObj3) && kObj3 is string[] kn ? kn : Array.Empty<string>();

                var keyTypes = m.AdditionalSettings.TryGetValue("keys/types", out var ktObj3) && ktObj3 is Type[] kt ? kt : Array.Empty<Type>();

                var valNames = m.AdditionalSettings.TryGetValue("projection", out var pObj3) && pObj3 is string[] vn ? vn : Array.Empty<string>();

                var valTypes = m.AdditionalSettings.TryGetValue("projection/types", out var vtObj3) && vtObj3 is Type[] vt ? vt : Array.Empty<Type>();

                var keyMeta = new Kafka.Ksql.Linq.Core.Models.PropertyMeta[Math.Min(keyNames.Length, keyTypes.Length)];

                for (int i = 0; i < keyMeta.Length; i++)

                {

                    keyMeta[i] = new Kafka.Ksql.Linq.Core.Models.PropertyMeta

                    {

                        Name = keyNames[i],

                        SourceName = keyNames[i],

                        PropertyType = keyTypes[i],

                        IsNullable = false,

                        Attributes = Array.Empty<Attribute>()

                    };

                }

                var valMeta = new Kafka.Ksql.Linq.Core.Models.PropertyMeta[valNames.Length];

                for (int i = 0; i < valNames.Length && i < valTypes.Length; i++)

                {

                    valMeta[i] = new Kafka.Ksql.Linq.Core.Models.PropertyMeta

                    {

                        Name = valNames[i],

                        SourceName = valNames[i],

                        PropertyType = valTypes[i],

                        IsNullable = true,

                        Attributes = Array.Empty<Attribute>()

                    };

                }

                mapping.RegisterMeta(dt, (keyMeta, valMeta), m.TopicName, genericKey: false, genericValue: true, overrideNamespace: ns);

            }

            catch { }

            registry[dt] = m;

        }



        static Kafka.Ksql.Linq.Runtime.Period ParsePeriod(string tf)

        {

            if (string.IsNullOrWhiteSpace(tf)) return Kafka.Ksql.Linq.Runtime.Period.Minutes(1);

            if (tf.EndsWith("mo", StringComparison.OrdinalIgnoreCase))

            {

                if (!int.TryParse(tf[..^2], out var vm)) vm = 1;

                return Kafka.Ksql.Linq.Runtime.Period.Months(vm);

            }

            if (tf.EndsWith("wk", StringComparison.OrdinalIgnoreCase))

            {

                return Kafka.Ksql.Linq.Runtime.Period.Week();

            }

            var unit = char.ToLowerInvariant(tf[^1]);

            if (!int.TryParse(tf[..^1], out var v)) v = 1;

            return unit switch

            {

                's' => Kafka.Ksql.Linq.Runtime.Period.Seconds(v),

                'm' => Kafka.Ksql.Linq.Runtime.Period.Minutes(v),

                'h' => Kafka.Ksql.Linq.Runtime.Period.Hours(v),

                'd' => Kafka.Ksql.Linq.Runtime.Period.Days(v),

                _ => Kafka.Ksql.Linq.Runtime.Period.Minutes(1)

            };

        }

        return executions;
    }



    public static IReadOnlyList<DerivedEntity> PlanDerivedEntities(TumblingQao qao, EntityModel model, bool whenEmpty)

        => DerivationPlanner.Plan(qao, model, whenEmpty);



    public static IReadOnlyList<EntityModel> AdaptModels(IReadOnlyList<DerivedEntity> entities)

        => EntityModelAdapter.Adapt(entities);



    private static (string ddl, Type entityType, string? ns, string? inputOverride) BuildDdlAndRegister(

        string baseName,

        KsqlQueryModel queryModel,

        EntityModel model,

        Role role,

        Func<string, Type> resolveType)

    {

        var qm = queryModel.Clone();

        string? inputOverride = null;

        // Respect explicit input hints when provided (e.g., hub stream/table linkage)

        if (model.AdditionalSettings.TryGetValue("input", out var inputObj))

        {

            inputOverride = inputObj?.ToString();

        }

        if (role == Role.Prev1m || role == Role.Final1sStream)

        {

            qm.Windows.Clear();

            qm.GroupByExpression = null;

            var inputType = resolveType(inputOverride ?? baseName);

            qm.SelectProjection = BuildInputProjection(inputType);

        }

        var tf = (string)model.AdditionalSettings["timeframe"];

        // If aggregating from hub stream, we must re-aggregate over hub columns (Open/High/Low/Close).

        // Expression tree rewriting cannot safely change delegate types; perform SELECT-clause textual adjustment.

        var needHubRewrite = role == Role.Live && !string.IsNullOrWhiteSpace(inputOverride) && inputOverride.EndsWith("_1s_final_s", StringComparison.OrdinalIgnoreCase);

        // Infer bucket column name when not present on the query model (e.g., tests building models directly)

        static string? InferBucketColumnName(KsqlQueryModel m, EntityModel em)

        {

            if (!string.IsNullOrWhiteSpace(m.BucketColumnName)) return m.BucketColumnName;

            static string? Pick(IEnumerable<string> cols)

            {

                var exact = cols.FirstOrDefault(x => string.Equals(x, "BucketStart", StringComparison.OrdinalIgnoreCase));

                if (!string.IsNullOrEmpty(exact)) return exact;

                return cols.FirstOrDefault(x => !string.IsNullOrEmpty(x) && x.IndexOf("bucket", StringComparison.OrdinalIgnoreCase) >= 0);

            }

            if (em.AdditionalSettings.TryGetValue("projection", out var pObj) && pObj is string[] projection)

            {

                var c = Pick(projection);

                if (!string.IsNullOrWhiteSpace(c)) return c;

            }

            if (em.AdditionalSettings.TryGetValue("keys", out var kObj) && kObj is string[] keys)

            {

                var c = Pick(keys);

                if (!string.IsNullOrWhiteSpace(c)) return c;

            }

            return null;

        }

        var inferredBucket = InferBucketColumnName(qm, model);

        var spec = RoleTraits.For(role);

        var emit = spec.Emit != null ? $"EMIT {spec.Emit}" : null;

        var name = role switch

        {

            Role.Live => $"{baseName}_{tf}_live",

            Role.Final => $"{baseName}_{tf}_final",

            Role.Final1s => $"{baseName}_{tf}_final",

            Role.Final1sStream => $"{baseName}_{tf}_final_s",

            Role.Prev1m => $"{baseName}_prev_1m",

            Role.Hb => $"{baseName}_hb_{tf}",

            Role.Fill => $"{baseName}_{tf}_fill",

            _ => $"{baseName}_{tf}"

        };

        string ddl;

        if (role == Role.Fill)

        {

            // Experimental: Build Fill DDL by driving from HB and left-joining live.

            // Note: prev_1m join and filler specifics will be added in a later pass.

            var keys = model.AdditionalSettings.TryGetValue("keys", out var kObj) ? (string[])kObj! : Array.Empty<string>();

            var projection = model.AdditionalSettings.TryGetValue("projection", out var pObj) ? (string[])pObj! : Array.Empty<string>();

            var bucketCol = inferredBucket ?? throw new InvalidOperationException("WhenEmpty/Fill requires WindowStart() in Select to define the bucket column.");

            // Heartbeat は各足ごと（WhenEmpty時）に生成される前提

            var hbName = $"{baseName}_hb_{tf}";

            var liveName = $"{baseName}_{tf}_live";

            // Optionally include prev_1m when timeframe is 1m to enable previous-close fill

            string? prevName = tf.Equals("1m", StringComparison.OrdinalIgnoreCase) ? $"{baseName}_prev_1m" : null;

            ddl = KsqlFillStatementBuilder.Build(name, keys, projection, bucketCol, hbName, liveName, prevName);

            if (!string.IsNullOrWhiteSpace(emit) && !ddl.Contains("EMIT ", StringComparison.OrdinalIgnoreCase))

                ddl = ddl.Replace(";", $" {emit};");

        }

        else if (role == Role.Prev1m)

        {

            var keys = model.AdditionalSettings.TryGetValue("keys", out var kObj2) ? (string[])kObj2! : Array.Empty<string>();

            var projection2 = model.AdditionalSettings.TryGetValue("projection", out var pObj2) ? (string[])pObj2! : Array.Empty<string>();

            var bucketCol2 = inferredBucket ?? throw new InvalidOperationException("Prev requires WindowStart() in Select to define the bucket column.");

            // 1m の前回値結合は同じ足の HB を使用

            var hbName2 = $"{baseName}_hb_{tf}"; // tf は 1m

            var liveName2 = $"{baseName}_{tf}_live";

            ddl = KsqlPrevStatementBuilder.Build(name, keys, projection2, bucketCol2, hbName2, liveName2, 1);

            if (!string.IsNullOrWhiteSpace(emit) && !ddl.Contains("EMIT ", StringComparison.OrdinalIgnoreCase))

                ddl = ddl.Replace(";", $" {emit};");

        }

        else if (role == Role.Final1sStream)

        {

            // ksqlDB limitation: persistent queries cannot source from windowed TABLEs.

            // Define a logical STREAM over the 1s TABLE's Kafka topic with explicit columns so keys are resolvable.

            var table1s = $"{baseName}_1s_final";

            // Build columns from AdditionalSettings (shapes captured earlier)

            var keyNames = model.AdditionalSettings.TryGetValue("keys", out var kObj) && kObj is string[] ks ? ks : Array.Empty<string>();

            var keyTypes = model.AdditionalSettings.TryGetValue("keys/types", out var ktObj) && ktObj is Type[] kts ? kts : Array.Empty<Type>();

            var valNames = model.AdditionalSettings.TryGetValue("projection", out var pObj) && pObj is string[] vs ? vs : Array.Empty<string>();

            var valTypes = model.AdditionalSettings.TryGetValue("projection/types", out var vtObj) && vtObj is Type[] vts ? vts : Array.Empty<Type>();

            static string Map(Type t) => Query.Schema.KsqlTypeMapping.MapToKsqlType(t, null);

            var cols = new System.Collections.Generic.List<string>();

            for (int i = 0; i < keyNames.Length && i < keyTypes.Length; i++)

                cols.Add($"{keyNames[i].ToUpperInvariant()} {Map(keyTypes[i])} KEY");

            for (int i = 0; i < valNames.Length && i < valTypes.Length; i++)

            {

                var n = valNames[i].ToUpperInvariant();

                if (keyNames.Any(k => string.Equals(k, valNames[i], StringComparison.OrdinalIgnoreCase)))

                    continue;

                cols.Add($"{n} {Map(valTypes[i])}");

            }

            var colList = string.Join(", ", cols);

            // Include PARTITIONS/REPLICAS so the KAFKA_TOPIC is created if missing (first run ordering tolerance)

            ddl = $"CREATE STREAM {name} ({colList}) WITH (KAFKA_TOPIC='{table1s}', KEY_FORMAT='AVRO', VALUE_FORMAT='AVRO', PARTITIONS=1, REPLICAS=1);";

        }

        else

        {

            // Generate windowed CTAS/CSAS. Keep key path style default for streams (alias-qualified o.KEYCOLS)

            ddl = KsqlCreateWindowedStatementBuilder.Build(name, qm, tf, emit, inputOverride, options: null);

            // 検査は行わない（ビルダー読み替えの責務に一本化し、

            // 不整合があれば ksqlDB のエラーをそのまま表面化させる）

            // ユーザーの ToQuery で宣言した SelectProjection は尊重する（ここで書き換えない）。

            // Inject GRACE PERIOD for Live windows based on the model-provided graceSeconds

            if (role == Role.Live)

            {

                var grace = 0;

                if (model.AdditionalSettings.TryGetValue("graceSeconds", out var gObj) && gObj is int g)

                    grace = g;

                if (grace > 0 && ddl.IndexOf("GRACE", StringComparison.OrdinalIgnoreCase) < 0)

                {

                    ddl = System.Text.RegularExpressions.Regex.Replace(

                        ddl,

                        @"(WINDOW\s+TUMBLING\s*\(\s*SIZE\s+[^\)]+)\)",

                        $"$1, GRACE PERIOD {grace} SECONDS)",

                        System.Text.RegularExpressions.RegexOptions.IgnoreCase);

                }

            }

        }

        var dt = resolveType(name);

        model.EntityType = dt;

        model.TopicName = name;

        model.SetStreamTableType(qm.DetermineType());

        var ns = model.AdditionalSettings.TryGetValue("namespace", out var nsObj) ? nsObj?.ToString() : null;

        return (ddl, dt, ns, inputOverride);

    }



    private static LambdaExpression BuildInputProjection(Type inputType)

    {

        // App-agnostic: select all columns (identity -> SELECT *)

        var p = Expression.Parameter(inputType, "x");

        return Expression.Lambda(p, p);

    }



    // Note: HubAggregationRewriter (expression-tree) was removed in favor of safe SELECT-clause adjustment above.
}
