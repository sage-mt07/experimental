using Kafka.Ksql.Linq.Query.Dsl;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace Kafka.Ksql.Linq.Query.Builders;

public static class KsqlInsertStatementBuilder
{
    public static string Build(string targetName, KsqlQueryModel model, Func<Type, string>? sourceNameResolver = null)
    {
        if (string.IsNullOrWhiteSpace(targetName))
            throw new ArgumentException("Target name is required", nameof(targetName));
        if (model == null)
            throw new ArgumentNullException(nameof(model));

        var groupByClause = BuildGroupByClause(model.GroupByExpression);

        string selectClause;
        if (model.SelectProjection == null)
        {
            selectClause = "*";
        }
        else
        {
            var map = new Dictionary<string, string>(StringComparer.Ordinal);
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

        var fromClause = BuildFromClauseCore(model, sourceNameResolver ?? ResolveSourceName);
        var whereClause = BuildWhereClause(model.WhereCondition, model);
        var havingClause = BuildHavingClause(model.HavingCondition);

        var sb = new StringBuilder();
        sb.Append($"INSERT INTO {targetName} ");
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

    private static string BuildFromClauseCore(KsqlQueryModel model, Func<Type, string>? sourceNameResolver)
    {
        var types = model.SourceTypes;
        if (types == null || types.Length == 0)
            throw new InvalidOperationException("Source types are required");

        if (types.Length > 2)
            throw new NotSupportedException("Only up to 2 tables are supported in JOIN");

        var result = new StringBuilder();
        var left = sourceNameResolver?.Invoke(types[0]) ?? ResolveSourceName(types[0]);
        var lAlias = "o";
        result.Append($"FROM {left} {lAlias}");

        if (types.Length > 1)
        {
            var right = sourceNameResolver?.Invoke(types[1]) ?? ResolveSourceName(types[1]);
            var rAlias = "i";
            result.Append($" JOIN {right} {rAlias}");
            if (model.JoinCondition == null)
                throw new InvalidOperationException("Join condition required for two table join");

            int withinSeconds;
            if (model.WithinSeconds.HasValue && model.WithinSeconds.Value > 0)
            {
                withinSeconds = model.WithinSeconds.Value;
            }
            else if (!model.ForbidDefaultWithin)
            {
                withinSeconds = 300;
            }
            else
            {
                throw new InvalidOperationException("Stream-Stream JOIN requires explicit Within(...) when default is disabled.");
            }
            result.Append($" WITHIN {withinSeconds} SECONDS");

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
                    return Common.BuilderValidation.SafeToString(ce.Value);
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
        var attr = type.GetCustomAttributes(true).OfType<Kafka.Ksql.Linq.Core.Attributes.KsqlTopicAttribute>().FirstOrDefault();
        if (attr != null && !string.IsNullOrWhiteSpace(attr.Name))
            return attr.Name.ToUpperInvariant();
        return type.Name;
    }

    private static string BuildWhereClause(LambdaExpression? where, KsqlQueryModel model)
    {
        if (where == null) return string.Empty;
        IDictionary<string, string>? map = null;
        if (where.Parameters != null && where.Parameters.Count > 0)
        {
            map = new Dictionary<string, string>(StringComparer.Ordinal);
            if (where.Parameters.Count > 0) map[where.Parameters[0].Name ?? string.Empty] = "o";
            if (where.Parameters.Count > 1) map[where.Parameters[1].Name ?? string.Empty] = "i";
        }
        var builder = map == null ? new WhereClauseBuilder() : new WhereClauseBuilder(map);
        var condition = builder.Build(where.Body);
        return $"WHERE {condition}";
    }

    private static string BuildGroupByClause(LambdaExpression? groupBy)
    {
        if (groupBy == null) return string.Empty;
        var builder = new GroupByClauseBuilder();
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
}
