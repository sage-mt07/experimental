using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Query.Dsl;
using Kafka.Ksql.Linq.Query.Abstractions;
using Kafka.Ksql.Linq.Query.Builders.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Text.RegularExpressions;
using System.Reflection;

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
        string? partitionClause = null;

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

        var fromClause = BuildFromClauseCore(model, sourceNameResolver, out var aliasToSource);
        var whereClause = BuildWhereClause(model.WhereCondition, model);
        var havingClause = BuildHavingClause(model.HavingCondition);

        var hasGroupBy = model.HasGroupBy();
        var hasWindow = model.HasTumbling();
        var hasEmitFinal = HasEmitFinal(model);
        var sourceTypes = model.SourceTypes ?? Array.Empty<Type>();
        var sourceIsStream = sourceTypes.Length > 0 && Array.TrueForAll(sourceTypes, t => !IsTableType(t));
        var partitionMergedIntoGroupBy = false;

        if (!string.IsNullOrWhiteSpace(partitionBy))
        {
            var normalizedPartition = NormalizePartitionClause(partitionBy);
            var partitionColumns = ExtractPartitionColumnKeys(normalizedPartition);
            if (partitionColumns.Count > 0)
            {
                var primaryType = sourceTypes.Length > 0 ? sourceTypes[0] : null;
                var primaryKeys = primaryType != null
                    ? ExtractKeyNames(primaryType)
                    : new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                var keysKnown = primaryKeys.Count > 0;
                var partitionMatchesKey = keysKnown
                    && partitionColumns.Count == primaryKeys.Count
                    && partitionColumns.All(primaryKeys.Contains);

                var singleSourceStream = sourceIsStream && sourceTypes.Length == 1;
                if (singleSourceStream
                    && !hasGroupBy
                    && !hasWindow
                    && !hasEmitFinal
                    && (!partitionMatchesKey || !keysKnown))
                {
                    partitionClause = normalizedPartition;
                }
            }
        }

        var aliasMetadata = BuildAliasMetadata(aliasToSource, sourceTypes);

        var keyMap = BuildKeyAliasMap(model, options?.KeyPathStyle ?? KeyPathStyle.None);
        var mapForFrom = keyMap.Where(kv => kv.Value.Style != KeyPathStyle.None)
            .ToDictionary(kv => kv.Key, kv => kv.Value);
        fromClause = ApplyKeyStyle(fromClause, mapForFrom);
        selectClause = ApplyKeyStyle(selectClause, keyMap);
        groupByClause = ApplyKeyStyle(groupByClause, keyMap);
        if (!string.IsNullOrWhiteSpace(partitionClause))
            partitionClause = ApplyKeyStyle(partitionClause, keyMap);
        whereClause = ApplyKeyStyle(whereClause, keyMap);
        havingClause = ApplyKeyStyle(havingClause, keyMap);

        DealiasClauses(aliasMetadata, ref selectClause, ref groupByClause, ref partitionClause, ref whereClause, ref havingClause);

        if (!string.IsNullOrWhiteSpace(partitionClause))
        {
            partitionClause = DeduplicatePartitionColumns(partitionClause);
        }

        if (!string.IsNullOrWhiteSpace(partitionClause))
        {
            groupByClause = MergeGroupByAndPartition(groupByClause, partitionClause!, out partitionMergedIntoGroupBy);
            partitionClause = null;
        }

        var createType = model.DetermineType() == StreamTableType.Table || partitionMergedIntoGroupBy
            ? "CREATE TABLE"
            : "CREATE STREAM";

        var sb = new StringBuilder();
        sb.Append($"{createType} {streamName} ");
        // Emit AVRO formats. KEY_FORMAT はキー定義がある場合のみ付与する（キー無しだとエラーになる環境があるため）
        var withParts = new List<string> { $"KAFKA_TOPIC='{streamName}'", "VALUE_FORMAT='AVRO'" };
        if (AnySourceHasKeys(model))
            withParts.Insert(1, "KEY_FORMAT='AVRO'");
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
        sb.AppendLine();
        sb.Append("EMIT CHANGES;");
        return sb.ToString();
    }

    private static bool AnySourceHasKeys(KsqlQueryModel model)
    {
        var types = model.SourceTypes ?? Array.Empty<Type>();
        foreach (var t in types)
        {
            var keys = ExtractKeyNames(t);
            if (keys != null && keys.Count > 0) return true;
        }
        return false;
    }

    private static string BuildFromClauseCore(KsqlQueryModel model, Func<Type, string>? sourceNameResolver, out Dictionary<string, string> aliasToSource)
    {
        var types = model.SourceTypes;
        if (types == null || types.Length == 0)
            throw new InvalidOperationException("Source types are required");

        if (types.Length > 2)
            throw new NotSupportedException("Only up to 2 tables are supported in JOIN");

        var result = new StringBuilder();
        aliasToSource = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var left = sourceNameResolver?.Invoke(types[0]) ?? ResolveSourceName(types[0]);
        var lAlias = "o"; // explicit alias for left source
        aliasToSource[lAlias] = left;
        result.Append($"FROM {left} {lAlias}");

        if (types.Length > 1)
        {
            var right = sourceNameResolver?.Invoke(types[1]) ?? ResolveSourceName(types[1]);
            var rAlias = "i"; // explicit alias for right source
            aliasToSource[rAlias] = right;
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

    private static bool HasEmitFinal(KsqlQueryModel model)
    {
        if (model.Extras != null && model.Extras.TryGetValue("emit", out var emitValue))
        {
            if (emitValue is string emitString && emitString.IndexOf("FINAL", StringComparison.OrdinalIgnoreCase) >= 0)
                return true;
        }
        return false;
    }

    private static bool IsTableType(Type type)
    {
        return type.GetCustomAttributes(typeof(KsqlTableAttribute), inherit: true).Length > 0;
    }

    private static string NormalizePartitionClause(string partitionClause)
    {
        if (string.IsNullOrWhiteSpace(partitionClause))
            return partitionClause;

        var parts = partitionClause.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        return string.Join(", ", parts);
    }

    private static List<string> ExtractPartitionColumnKeys(string partitionClause)
    {
        var keys = new List<string>();
        if (string.IsNullOrWhiteSpace(partitionClause))
            return keys;

        var parts = partitionClause.Split(',', StringSplitOptions.RemoveEmptyEntries);
        foreach (var part in parts)
        {
            var trimmed = part.Trim();
            if (trimmed.Length == 0)
                continue;

            var unqualified = ExtractUnqualifiedIdentifier(trimmed);
            var normalized = NormalizeIdentifierForComparison(unqualified);
            if (!string.IsNullOrEmpty(normalized))
                keys.Add(normalized);
        }

        return keys;
    }

    private static string DeduplicatePartitionColumns(string partitionClause)
    {
        if (string.IsNullOrWhiteSpace(partitionClause))
            return partitionClause;

        var parts = partitionClause.Split(',', StringSplitOptions.RemoveEmptyEntries);
        var tokens = new List<(string Original, string Unqualified, string Normalized, string Qualifier, int Index)>();
        for (int i = 0; i < parts.Length; i++)
        {
            var trimmed = parts[i].Trim();
            if (trimmed.Length == 0)
                continue;

            var unqualified = ExtractUnqualifiedIdentifier(trimmed);
            if (string.IsNullOrEmpty(unqualified))
                continue;

            var normalized = NormalizeIdentifierForComparison(unqualified);
            if (string.IsNullOrEmpty(normalized))
                continue;

            var qualifier = ExtractQualifier(trimmed);

            tokens.Add((trimmed, unqualified, normalized, qualifier, i));
        }

        if (tokens.Count == 0)
            return string.Empty;

        var ordered = tokens
            .OrderBy(t => t.Normalized, StringComparer.OrdinalIgnoreCase)
            .ThenBy(t => t.Qualifier, StringComparer.OrdinalIgnoreCase)
            .ThenBy(t => t.Index)
            .ToList();

        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var result = new List<string>();
        foreach (var token in ordered)
        {
            var key = BuildDedupKey(token.Normalized, token.Qualifier);
            if (seen.Add(key))
            {
                result.Add(string.IsNullOrEmpty(token.Qualifier) ? token.Unqualified : token.Original);
            }
        }

        return string.Join(", ", result);
    }

    private static string MergeGroupByAndPartition(string groupByClause, string partitionColumns, out bool merged)
    {
        merged = false;
        if (string.IsNullOrWhiteSpace(partitionColumns))
            return groupByClause;

        var groupColumns = ExtractGroupByColumns(groupByClause);
        var known = new HashSet<string>(groupColumns.Select(c => NormalizeGroupByKey(c)), StringComparer.OrdinalIgnoreCase);

        var partitionList = ExtractColumns(partitionColumns);
        foreach (var column in partitionList)
        {
            var key = NormalizeGroupByKey(column);
            if (known.Add(key))
                groupColumns.Add(column.Trim());
        }

        merged = partitionList.Count > 0;
        if (groupColumns.Count == 0)
            return string.Empty;

        return "GROUP BY " + string.Join(", ", groupColumns);
    }

    private static void DealiasClauses(Dictionary<string, SourceAliasMetadata> aliasMetadata, ref string selectClause, ref string groupByClause, ref string? partitionClause, ref string whereClause, ref string havingClause)
    {
        if (aliasMetadata == null || aliasMetadata.Count == 0)
            return;

        var ambiguousColumns = DetermineAmbiguousColumns(aliasMetadata.Values);

        selectClause = ApplyAliasPreferences(selectClause, aliasMetadata, ambiguousColumns);
        groupByClause = ApplyAliasPreferences(groupByClause, aliasMetadata, ambiguousColumns);
        if (!string.IsNullOrWhiteSpace(partitionClause))
            partitionClause = ApplyAliasPreferences(partitionClause!, aliasMetadata, ambiguousColumns);
        whereClause = ApplyAliasPreferences(whereClause, aliasMetadata, ambiguousColumns);
        havingClause = ApplyAliasPreferences(havingClause, aliasMetadata, ambiguousColumns);
    }

    private static string ApplyAliasPreferences(string clause, Dictionary<string, SourceAliasMetadata> aliasMetadata, HashSet<string> ambiguousColumns)
    {
        if (string.IsNullOrWhiteSpace(clause))
            return clause;

        foreach (var metadata in aliasMetadata.Values)
        {
            clause = ReplaceAliasWithPreferredScope(clause, metadata, ambiguousColumns);
        }

        return clause;
    }

    private static string ReplaceAliasWithPreferredScope(string clause, SourceAliasMetadata metadata, HashSet<string> ambiguousColumns)
    {
        if (string.IsNullOrWhiteSpace(clause))
            return clause;

        var aliasPattern = Regex.Escape(metadata.Alias);

        clause = ReplaceAliasPattern(clause, $@"\b{aliasPattern}\.\`(?<column>[A-Za-z0-9_]+)\`", metadata, ambiguousColumns, '`');
        clause = ReplaceAliasPattern(clause, $@"\b{aliasPattern}\.""(?<column>[A-Za-z0-9_]+)""", metadata, ambiguousColumns, '"');
        clause = ReplaceAliasPattern(clause, $@"\b{aliasPattern}\.(?<column>[A-Za-z0-9_]+)", metadata, ambiguousColumns, null);

        return clause;
    }

    private static string ReplaceAliasPattern(string clause, string pattern, SourceAliasMetadata metadata, HashSet<string> ambiguousColumns, char? quote)
    {
        return Regex.Replace(clause, pattern, match =>
        {
            var column = match.Groups["column"].Value;
            var normalized = NormalizeIdentifierForComparison(column);
            var columnWithQuote = quote switch
            {
                '`' => $"`{column}`",
                '"' => $"\"{column}\"",
                _ => column
            };

            if (ambiguousColumns.Contains(normalized))
                return $"{metadata.SourceName}.{columnWithQuote}";

            return columnWithQuote;
        }, RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
    }

    private static HashSet<string> DetermineAmbiguousColumns(IEnumerable<SourceAliasMetadata> aliasMetadata)
    {
        var counts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        foreach (var metadata in aliasMetadata)
        {
            foreach (var column in metadata.ColumnIdentifiers)
            {
                if (string.IsNullOrEmpty(column))
                    continue;

                counts[column] = counts.TryGetValue(column, out var existing) ? existing + 1 : 1;
            }
        }

        return counts
            .Where(kv => kv.Value > 1)
            .Select(kv => kv.Key)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
    }

    private static string ExtractUnqualifiedIdentifier(string expression)
    {
        if (string.IsNullOrWhiteSpace(expression))
            return string.Empty;

        var value = expression.Trim();
        var arrowIndex = value.LastIndexOf("->");
        if (arrowIndex >= 0)
            value = value[(arrowIndex + 2)..];

        var dotIndex = value.LastIndexOf('.');
        if (dotIndex >= 0)
            value = value[(dotIndex + 1)..];

        return value.Trim();
    }

    private static string NormalizeIdentifierForComparison(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
            return string.Empty;

        var value = identifier.Trim();
        if (value.EndsWith("()"))
            value = value[..^2];

        if (value.Length >= 2 && ((value[0] == '`' && value[^1] == '`') || (value[0] == '"' && value[^1] == '"')))
            value = value[1..^1];

        return value.ToUpperInvariant();
    }

    private static List<string> ExtractGroupByColumns(string groupByClause)
    {
        if (string.IsNullOrWhiteSpace(groupByClause))
            return new List<string>();

        var value = groupByClause.Trim();
        if (value.StartsWith("GROUP BY", StringComparison.OrdinalIgnoreCase))
            value = value.Substring("GROUP BY".Length).Trim();

        return ExtractColumns(value);
    }

    private static List<string> ExtractColumns(string columns)
    {
        if (string.IsNullOrWhiteSpace(columns))
            return new List<string>();

        return columns
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(c => c.Trim())
            .Where(c => c.Length > 0)
            .ToList();
    }

    private static string NormalizeGroupByKey(string column)
    {
        return NormalizeIdentifierForComparison(ExtractUnqualifiedIdentifier(column));
    }

    private static string ExtractQualifier(string expression)
    {
        if (string.IsNullOrWhiteSpace(expression))
            return string.Empty;

        var value = expression.Trim();
        var arrowIndex = value.LastIndexOf("->");
        if (arrowIndex >= 0)
        {
            var qualifier = value[..arrowIndex];
            return qualifier.Trim();
        }

        var dotIndex = value.LastIndexOf('.');
        if (dotIndex >= 0)
        {
            var qualifier = value[..dotIndex];
            return qualifier.Trim();
        }

        return string.Empty;
    }

    private static string BuildDedupKey(string normalized, string qualifier)
    {
        if (string.IsNullOrEmpty(qualifier))
            return normalized;

        return qualifier.ToUpperInvariant() + "::" + normalized;
    }

    private static Dictionary<string, SourceAliasMetadata> BuildAliasMetadata(Dictionary<string, string> aliasToSource, Type[] sourceTypes)
    {
        var map = new Dictionary<string, SourceAliasMetadata>(StringComparer.OrdinalIgnoreCase);
        if (aliasToSource == null || aliasToSource.Count == 0)
            return map;

        foreach (var kv in aliasToSource)
        {
            var alias = kv.Key;
            var sourceName = kv.Value;
            var sourceType = alias switch
            {
                "o" => sourceTypes.Length > 0 ? sourceTypes[0] : null,
                "i" => sourceTypes.Length > 1 ? sourceTypes[1] : null,
                _ => null
            };

            map[alias] = new SourceAliasMetadata(alias, sourceName, sourceType);
        }

        return map;
    }

    private static HashSet<string> ExtractColumnIdentifiers(Type? type)
    {
        var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (type == null)
            return set;

        foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            var sanitized = KsqlNameUtils.Sanitize(property.Name);
            var normalized = NormalizeIdentifierForComparison(sanitized);
            if (!string.IsNullOrEmpty(normalized))
                set.Add(normalized);
        }

        return set;
    }

    private sealed class SourceAliasMetadata
    {
        public SourceAliasMetadata(string alias, string sourceName, Type? sourceType)
        {
            Alias = alias;
            SourceName = sourceName;
            SourceType = sourceType;
            ColumnIdentifiers = ExtractColumnIdentifiers(sourceType);
        }

        public string Alias { get; }
        public string SourceName { get; }
        public Type? SourceType { get; }
        public HashSet<string> ColumnIdentifiers { get; }
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
