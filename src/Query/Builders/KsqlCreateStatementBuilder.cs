using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Query.Dsl;
using Kafka.Ksql.Linq.Query.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace Kafka.Ksql.Linq.Query.Builders;

public static class KsqlCreateStatementBuilder
{
    public static string Build(string streamName, KsqlQueryModel model, string? keySchemaFullName = null, string? valueSchemaFullName = null, string? partitionBy = null, RenderOptions? options = null)
    {
        return Build(streamName, model, keySchemaFullName, valueSchemaFullName, ResolveSourceName, partitionBy, options);
    }

    /// <summary>
    /// Build a CREATE statement with an optional source name resolver for FROM/JOIN tables.
    /// </summary>
    public static string Build(string streamName, KsqlQueryModel model, string? keySchemaFullName, string? valueSchemaFullName, Func<Type, string> sourceNameResolver, string? partitionBy = null, RenderOptions? options = null)
    {
        if (string.IsNullOrWhiteSpace(streamName))
            throw new ArgumentException("Stream name is required", nameof(streamName));
        if (model == null)
            throw new ArgumentNullException(nameof(model));

        var groupByClause = BuildGroupByClause(model.GroupByExpression, model.SourceTypes);

        string selectClause;
        if (model.SelectProjection == null)
        {
            selectClause = "*";
        }
        else
        {
            var map = new System.Collections.Generic.Dictionary<string, string>(StringComparer.Ordinal);
            var parameters = model.SelectProjection.Parameters;
            for (int i = 0; i < parameters.Count && i < (model.SourceTypes?.Length ?? 0); i++)
            {
                var pname = parameters[i].Name ?? string.Empty;
                var alias = i == 0 ? "o" : "i";
                map[pname] = alias;
            }
            var builder = new SelectClauseBuilder(map);
            selectClause = builder.Build(model.SelectProjection.Body);
        }

        var fromClause = BuildFromClauseCore(model, sourceNameResolver);
        var whereClause = BuildWhereClause(model.WhereCondition, model);
        var havingClause = BuildHavingClause(model.HavingCondition);

        var keyMap = BuildKeyAliasMap(model, options?.KeyPathStyle ?? KeyPathStyle.None);
        var mapForFrom = keyMap.Where(kv => kv.Value.Style != KeyPathStyle.None)
            .ToDictionary(kv => kv.Key, kv => kv.Value);
        fromClause = ApplyKeyStyle(fromClause, mapForFrom);
        selectClause = ApplyKeyStyle(selectClause, keyMap);
        groupByClause = ApplyKeyStyle(groupByClause, keyMap);
        whereClause = ApplyKeyStyle(whereClause, keyMap);
        havingClause = ApplyKeyStyle(havingClause, keyMap);

        var createType = model.DetermineType() == StreamTableType.Table ? "CREATE TABLE" : "CREATE STREAM";

        var sb = new StringBuilder();
        sb.Append($"{createType} {streamName}");
        // Emit AVRO formats; add only VALUE_AVRO_SCHEMA_FULL_NAME when provided (KEY_* is unsupported)
        var withParts = new List<string> { $"KAFKA_TOPIC='{streamName}'", "KEY_FORMAT='AVRO'", "VALUE_FORMAT='AVRO'" };
        if (!string.IsNullOrWhiteSpace(valueSchemaFullName))
            withParts.Add($"VALUE_AVRO_SCHEMA_FULL_NAME='{valueSchemaFullName}'");
        sb.Append(" WITH (" + string.Join(", ", withParts) + ")");
        sb.AppendLine(" AS");
        sb.AppendLine($"SELECT {selectClause}");
        sb.Append(fromClause);
        if (!string.IsNullOrEmpty(whereClause))
        {
            sb.AppendLine();
            sb.Append(whereClause);
        }
        if (!string.IsNullOrEmpty(groupByClause))
        {
            sb.AppendLine();
            sb.Append(groupByClause);
        }
        if (!string.IsNullOrEmpty(havingClause))
        {
            sb.AppendLine();
            sb.Append(havingClause);
        }
        if (!string.IsNullOrEmpty(partitionBy))
        {
            sb.AppendLine();
            sb.Append($"PARTITION BY {partitionBy}");
        }
        sb.AppendLine();
        sb.Append("EMIT CHANGES;");
        return sb.ToString();
    }

    private static string BuildFromClauseCore(KsqlQueryModel model, Func<Type, string>? sourceNameResolver)
    {
        var types = model.SourceTypes;
        if (types == null || types.Length == 0)
            throw new InvalidOperationException("Source types are required");

        if (types.Length > 2)
            throw new NotSupportedException("Only up to 2 tables are supported in JOIN");

        var result = new StringBuilder();
        var left = sourceNameResolver?.Invoke(types[0]) ?? ResolveSourceName(types[0]);
        var lAlias = "o"; // explicit alias for left source
        result.Append($"FROM {left} {lAlias}");

        if (types.Length > 1)
        {
            var right = sourceNameResolver?.Invoke(types[1]) ?? ResolveSourceName(types[1]);
            var rAlias = "i"; // explicit alias for right source
            result.Append($" JOIN {right} {rAlias}");
            if (model.JoinCondition == null)
                throw new InvalidOperationException("Join condition required for two table join");

            // Enforce WITHIN for stream-stream joins: allow default 300s unless forbidden
            int withinSeconds;
            if (model.WithinSeconds.HasValue && model.WithinSeconds.Value > 0)
            {
                withinSeconds = model.WithinSeconds.Value;
            }
            else if (!model.ForbidDefaultWithin)
            {
                withinSeconds = 300; // default
            }
            else
            {
                throw new InvalidOperationException("Stream-Stream JOIN requires explicit Within(...) when default is disabled.");
            }
            result.Append($" WITHIN {withinSeconds} SECONDS");

            // Build a qualified join condition using aliases to avoid ambiguity
            var condition = BuildQualifiedJoinCondition(model.JoinCondition, lAlias, rAlias);
            result.Append($" ON {condition}");
        }

        return result.ToString();
    }

    private static string BuildQualifiedJoinCondition(LambdaExpression joinExpr, string leftAlias, string rightAlias)
    {
        string Build(Expression expr)
        {
            switch (expr)
            {
                case BinaryExpression be when be.NodeType == ExpressionType.Equal:
                    return $"({Build(be.Left)} = {Build(be.Right)})";
                case MemberExpression me:
                    {
                        var param = GetRootParameter(me);
                        if (param != null)
                        {
                            if (joinExpr.Parameters.Count > 0 && param == joinExpr.Parameters[0])
                                return $"{leftAlias}.{me.Member.Name}";
                            if (joinExpr.Parameters.Count > 1 && param == joinExpr.Parameters[1])
                                return $"{rightAlias}.{me.Member.Name}";
                        }
                        throw new InvalidOperationException("Unqualified column access in JOIN condition is not allowed.");
                    }
                case UnaryExpression ue:
                    return Build(ue.Operand);
                case ConstantExpression ce:
                    return Builders.Common.BuilderValidation.SafeToString(ce.Value);
                default:
                    return expr.ToString();
            }
        }

        static ParameterExpression? GetRootParameter(MemberExpression me)
        {
            Expression? e = me.Expression;
            while (e is MemberExpression m)
                e = m.Expression;
            return e as ParameterExpression;
        }

        return Build(joinExpr.Body);
    }

    private static string ResolveSourceName(Type type)
    {
        // If the entity type has [KsqlTopic("name")], use that (uppercased for KSQL identifiers)
        var attr = type.GetCustomAttributes(true).OfType<Kafka.Ksql.Linq.Core.Attributes.KsqlTopicAttribute>().FirstOrDefault();
        if (attr != null && !string.IsNullOrWhiteSpace(attr.Name))
            return attr.Name.ToUpperInvariant();
        return type.Name;
    }

    private static string BuildWhereClause(LambdaExpression? where, KsqlQueryModel model)
    {
        if (where == null) return string.Empty;
        // Build parameter-to-alias map: first param -> o, second -> i
        System.Collections.Generic.IDictionary<string, string>? map = null;
        if (where.Parameters != null && where.Parameters.Count > 0)
        {
            map = new System.Collections.Generic.Dictionary<string, string>(System.StringComparer.Ordinal);
            if (where.Parameters.Count > 0) map[where.Parameters[0].Name ?? string.Empty] = "o";
            if (where.Parameters.Count > 1) map[where.Parameters[1].Name ?? string.Empty] = "i";
        }
        var builder = map == null ? new WhereClauseBuilder() : new WhereClauseBuilder(map);
        var condition = builder.Build(where.Body);
        return $"WHERE {condition}";
    }

    private static string BuildGroupByClause(LambdaExpression? groupBy, Type[]? sourceTypes)
    {
        if (groupBy == null) return string.Empty;

        var map = new System.Collections.Generic.Dictionary<string, string>(StringComparer.Ordinal);
        var parameters = groupBy.Parameters;
        for (int i = 0; i < parameters.Count && i < (sourceTypes?.Length ?? 0); i++)
        {
            var pname = parameters[i].Name ?? string.Empty;
            var alias = i == 0 ? "o" : "i";
            map[pname] = alias;
        }
        var builder = new GroupByClauseBuilder(map);
        var keys = builder.Build(groupBy.Body);
        return $"GROUP BY {keys}";
    }

    private static string BuildHavingClause(LambdaExpression? having)
    {
        if (having == null) return string.Empty;
        var builder = new HavingClauseBuilder();
        var condition = builder.Build(having.Body);
        return $"HAVING {condition}";
    }

    private static System.Collections.Generic.Dictionary<string, (System.Collections.Generic.HashSet<string> Keys, KeyPathStyle Style)> BuildKeyAliasMap(KsqlQueryModel model, KeyPathStyle overrideStyle)
    {
        var map = new System.Collections.Generic.Dictionary<string, (System.Collections.Generic.HashSet<string>, KeyPathStyle)>(StringComparer.OrdinalIgnoreCase);
        var types = model.SourceTypes ?? Array.Empty<Type>();
        if (types.Length > 0)
            map["o"] = (ExtractKeyNames(types[0]), DetermineStyle(types[0], overrideStyle));
        if (types.Length > 1)
            map["i"] = (ExtractKeyNames(types[1]), DetermineStyle(types[1], overrideStyle));
        // TODO: future model-provided aliases should feed this map instead of fixed o/i.
        return map;
    }

    private static KeyPathStyle DetermineStyle(Type type, KeyPathStyle overrideStyle)
    {
        if (overrideStyle != KeyPathStyle.None)
            return overrideStyle;
        // Auto-detection only yields Arrow for tables; Dot is reserved for explicit overrides.
        return type.GetCustomAttributes(true).OfType<KsqlTableAttribute>().Any()
            ? KeyPathStyle.Arrow
            : KeyPathStyle.None;
    }

    private static System.Collections.Generic.HashSet<string> ExtractKeyNames(Type type)
    {
        return type
            .GetProperties()
            .Where(p => p.GetCustomAttributes(true).OfType<KsqlKeyAttribute>().Any())
            .Select(p => p.Name.ToUpperInvariant())
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
    }

    private static string ApplyKeyStyle(string clause, System.Collections.Generic.Dictionary<string, (System.Collections.Generic.HashSet<string> Keys, KeyPathStyle Style)> map)
    {
        if (string.IsNullOrEmpty(clause)) return clause;
        foreach (var kv in map)
        {
            var alias = kv.Key;
            var keys = kv.Value.Keys;
            var style = kv.Value.Style;
            if (style == KeyPathStyle.None)
                continue; // keep original alias-qualified key paths for streams
            if (keys.Count == 1)
            {
                var lone = keys.First();
                var standAlonePattern = @"\bKEY\b";
                var standAloneReplacement = style switch
                {
                    KeyPathStyle.Dot => $"key.{lone}",
                    KeyPathStyle.Arrow => $"KEY->{lone}",
                    _ => lone
                };
                clause = System.Text.RegularExpressions.Regex.Replace(
                    clause,
                    standAlonePattern,
                    standAloneReplacement,
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Compiled | System.Text.RegularExpressions.RegexOptions.CultureInvariant);
            }
            foreach (var key in keys)
            {
                // Skip tokens already prefixed (KEY-> or key.) and any inside quotes/backticks.
                var pattern = $@"(?<!KEY->)(?<!key\.)(?<![`'""])\b{alias}\.{key}\b(?![`'""])";
                var replacement = style switch
                {
                    KeyPathStyle.Dot => $"key.{key}",
                    KeyPathStyle.Arrow => $"KEY->{key}",
                    _ => key
                };
                clause = System.Text.RegularExpressions.Regex.Replace(
                    clause,
                    pattern,
                    replacement,
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Compiled | System.Text.RegularExpressions.RegexOptions.CultureInvariant);
            }
        }
        return clause;
    }

    private static string FormatTimeSpan(TimeSpan timeSpan)
    {
        if (timeSpan.TotalDays >= 1 && timeSpan.TotalDays == Math.Floor(timeSpan.TotalDays))
            return $"{(int)timeSpan.TotalDays} DAYS";
        if (timeSpan.TotalHours >= 1 && timeSpan.TotalHours == Math.Floor(timeSpan.TotalHours))
            return $"{(int)timeSpan.TotalHours} HOURS";
        if (timeSpan.TotalMinutes >= 1 && timeSpan.TotalMinutes == Math.Floor(timeSpan.TotalMinutes))
            return $"{(int)timeSpan.TotalMinutes} MINUTES";
        if (timeSpan.TotalSeconds >= 1 && timeSpan.TotalSeconds == Math.Floor(timeSpan.TotalSeconds))
            return $"{(int)timeSpan.TotalSeconds} SECONDS";
        if (timeSpan.TotalMilliseconds >= 1)
            return $"{(int)timeSpan.TotalMilliseconds} MILLISECONDS";
        return "0 SECONDS";
    }
}
