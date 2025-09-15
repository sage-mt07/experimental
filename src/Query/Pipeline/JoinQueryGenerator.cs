using Kafka.Ksql.Linq.Query.Abstractions;
using Kafka.Ksql.Linq.Query.Builders;
using Kafka.Ksql.Linq.Query.Builders.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Kafka.Ksql.Linq.Query.Pipeline;

/// <summary>
/// JOIN query generator (newly created)
/// Rationale: separation-of-concerns; specialized for 2-table JOIN and LEFT JOIN
/// </summary>
internal class JoinQueryGenerator : GeneratorBase
{
    /// <summary>
    /// Constructor (builder dependency injection)
    /// </summary>
    public JoinQueryGenerator(IReadOnlyDictionary<KsqlBuilderType, IKsqlBuilder> builders)
        : base(builders)
    {
    }

    /// <summary>
    /// Simplified constructor (uses standard builders)
    /// </summary>
    public JoinQueryGenerator() : this(CreateStandardBuilders())
    {
    }

    protected override KsqlBuilderType[] GetRequiredBuilderTypes()
    {
        return new[]
        {
            KsqlBuilderType.Join,
            KsqlBuilderType.Select,
            KsqlBuilderType.Where
        };
    }

    /// <summary>
    /// Generate two-table JOIN query
    /// </summary>
    public string GenerateTwoTableJoin(
        string outerTable,
        string innerTable,
        Expression outerKeySelector,
        Expression innerKeySelector,
        Expression? resultSelector = null,
        Expression? whereCondition = null,
        bool isPullQuery = true)
    {
        try
        {
            // Check join constraints
            ValidateJoinConstraints(outerTable, innerTable);

            var context = new QueryAssemblyContext($"{outerTable}_JOIN_{innerTable}", isPullQuery);
            var structure = CreateJoinStructure(outerTable, innerTable);

            // Build JOIN condition
            var joinExpression = BuildJoinExpression(outerTable, innerTable, outerKeySelector, innerKeySelector, resultSelector);
            var joinContent = SafeCallBuilder(KsqlBuilderType.Join, joinExpression, "JOIN processing");

            // JOIN clause is returned as a complete query (JoinClauseBuilder generates full SELECT)
            return ApplyQueryPostProcessing(joinContent, context);
        }
        catch (System.Exception ex)
        {
            return HandleGenerationError("two-table JOIN generation", ex, $"Tables: {outerTable}, {innerTable}");
        }
    }

    /// <summary>
    /// Generate query from LINQ JOIN expression
    /// </summary>
    public string GenerateFromLinqJoin(Expression joinExpression, bool isPullQuery = true)
    {
        try
        {
            // Pre-check join constraints
            JoinLimitationEnforcer.ValidateJoinExpression(joinExpression);

            var context = new QueryAssemblyContext("LINQ_JOIN", isPullQuery);

            // Delegate to JoinBuilder to generate the full query
            var joinQuery = SafeCallBuilder(KsqlBuilderType.Join, joinExpression, "LINQ JOIN processing");

            return ApplyQueryPostProcessing(joinQuery, context);
        }
        catch (System.Exception ex)
        {
            return HandleGenerationError("LINQ JOIN generation", ex, joinExpression.ToString());
        }
    }

    /// <summary>
    /// Generate LEFT JOIN query (within KSQL capabilities)
    /// </summary>
    public string GenerateLeftJoin(
        string outerTable,
        string innerTable,
        Expression outerKeySelector,
        Expression innerKeySelector,
        Expression? resultSelector = null,
        bool isPullQuery = true)
    {
        try
        {
            ValidateJoinConstraints(outerTable, innerTable);

            var context = new QueryAssemblyContext($"{outerTable}_LEFT_JOIN_{innerTable}", isPullQuery);

            // Manually build LEFT JOIN due to KSQL limitations
            var query = BuildLeftJoinQuery(outerTable, innerTable, outerKeySelector, innerKeySelector, resultSelector);

            return ApplyQueryPostProcessing(query, context);
        }
        catch (System.Exception ex)
        {
            return HandleGenerationError("LEFT JOIN generation", ex, $"Tables: {outerTable}, {innerTable}");
        }
    }

    /// <summary>
    /// Validate join conditions
    /// </summary>
    private static void ValidateJoinConstraints(string outerTable, string innerTable)
    {
        if (string.IsNullOrWhiteSpace(outerTable))
            throw new ArgumentException("Outer table name cannot be null or empty");

        if (string.IsNullOrWhiteSpace(innerTable))
            throw new ArgumentException("Inner table name cannot be null or empty");

        if (outerTable.Equals(innerTable, StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException("Cannot join table with itself in KSQL");
    }

    /// <summary>
    /// Create JOIN structure
    /// </summary>
    private static QueryStructure CreateJoinStructure(string outerTable, string innerTable)
    {
        var metadata = new QueryMetadata(DateTime.UtcNow, "DML", $"{outerTable}_JOIN_{innerTable}");
        return QueryStructure.CreateSelect($"{outerTable}_JOIN_{innerTable}").WithMetadata(metadata);
    }

    /// <summary>
    /// Build join expression
    /// </summary>
    private Expression BuildJoinExpression(
        string outerTable,
        string innerTable,
        Expression outerKeySelector,
        Expression innerKeySelector,
        Expression? resultSelector)
    {
        // Extract key selector lambda and obtain type info
        var outerLambda = ExtractLambdaExpression(outerKeySelector)
            ?? throw new InvalidOperationException("Outer key selector must be a lambda expression");
        var innerLambda = ExtractLambdaExpression(innerKeySelector)
            ?? throw new InvalidOperationException("Inner key selector must be a lambda expression");

        var outerType = outerLambda.Parameters[0].Type;
        var innerType = innerLambda.Parameters[0].Type;
        var keyType = outerLambda.Body.Type;

        // For IQueryable<> parameters, use only type information
        var outerQueryable = Expression.Parameter(typeof(IQueryable<>).MakeGenericType(outerType), "outer");
        var innerQueryable = Expression.Parameter(typeof(IQueryable<>).MakeGenericType(innerType), "inner");

        var outerTypedLambda = Expression.Lambda(
            typeof(Func<,>).MakeGenericType(outerType, keyType),
            outerLambda.Body,
            outerLambda.Parameters);
        var innerTypedLambda = Expression.Lambda(
            typeof(Func<,>).MakeGenericType(innerType, keyType),
            innerLambda.Body,
            innerLambda.Parameters);

        var joinMethod = typeof(Queryable).GetMethods()
            .First(m => m.Name == nameof(Queryable.Join) && m.GetParameters().Length == 5)
            .MakeGenericMethod(outerType, innerType, keyType, typeof(object));

        var defaultSelector = resultSelector ??
            CreateDefaultResultSelector(Expression.Parameter(outerType, outerLambda.Parameters[0].Name!),
                Expression.Parameter(innerType, innerLambda.Parameters[0].Name!));

        return Expression.Call(
            joinMethod,
            outerQueryable,
            innerQueryable,
            outerTypedLambda,
            innerTypedLambda,
            defaultSelector);
    }

    /// <summary>
    /// Extract lambda expression
    /// </summary>
    private static LambdaExpression? ExtractLambdaExpression(Expression expr)
    {
        return expr switch
        {
            UnaryExpression { Operand: LambdaExpression lambda } => lambda,
            LambdaExpression lambda => lambda,
            _ => null
        };
    }

    /// <summary>
    /// Create default ResultSelector
    /// </summary>
    private static Expression CreateDefaultResultSelector(ParameterExpression outerParam, ParameterExpression innerParam)
    {
        // Default returns all columns from both tables
        var newExpression = Expression.New(typeof(object).GetConstructors()[0]);
        return Expression.Lambda(newExpression, outerParam, innerParam);
    }

    /// <summary>
    /// Construct LEFT JOIN query
    /// </summary>
    private string BuildLeftJoinQuery(
        string outerTable,
        string innerTable,
        Expression outerKeySelector,
        Expression innerKeySelector,
        Expression? resultSelector)
    {
        var outerKeys = ExtractKeys(outerKeySelector);
        var innerKeys = ExtractKeys(innerKeySelector);

        var outerAlias = "o";
        var innerAlias = "i";

        var joinConditions = BuildJoinConditions(outerKeys, innerKeys, outerAlias, innerAlias);

        var projection = resultSelector != null ?
            ProcessResultSelector(resultSelector, outerAlias, innerAlias) :
            $"{outerAlias}.*, {innerAlias}.*";

        return $"SELECT {projection} " +
               $"FROM {outerTable} {outerAlias} " +
               $"LEFT JOIN {innerTable} {innerAlias} ON {joinConditions}";
    }

    /// <summary>
    /// Extract keys
    /// </summary>
    private List<string> ExtractKeys(Expression keySelector)
    {
        var keys = new List<string>();

        var lambdaBody = BuilderValidation.ExtractLambdaBody(keySelector);
        if (lambdaBody == null) return keys;

        switch (lambdaBody)
        {
            case NewExpression newExpr:
                foreach (var arg in newExpr.Arguments)
                {
                    if (arg is MemberExpression member)
                    {
                        keys.Add(member.Member.Name);
                    }
                }
                break;

            case MemberExpression member:
                keys.Add(member.Member.Name);
                break;
        }

        return keys;
    }

    /// <summary>
    /// Build join conditions
    /// </summary>
    private static string BuildJoinConditions(List<string> leftKeys, List<string> rightKeys, string leftAlias, string rightAlias)
    {
        if (leftKeys.Count != rightKeys.Count || leftKeys.Count == 0)
        {
            throw new InvalidOperationException("JOIN keys must match and cannot be empty");
        }

        var conditions = new List<string>();
        for (int i = 0; i < leftKeys.Count; i++)
        {
            conditions.Add($"{leftAlias}.{leftKeys[i]} = {rightAlias}.{rightKeys[i]}");
        }

        return string.Join(" AND ", conditions);
    }

    /// <summary>
    /// Process ResultSelector
    /// </summary>
    private string ProcessResultSelector(Expression resultSelector, params string[] tableAliases)
    {
        // Simplification: actual implementation uses SelectClauseBuilder
        var lambdaBody = BuilderValidation.ExtractLambdaBody(resultSelector);
        if (lambdaBody != null)
        {
            try
            {
                return SafeCallBuilder(KsqlBuilderType.Select, lambdaBody, "result selector processing");
            }
            catch
            {
                // Fallback: return all columns
                return string.Join(", ", tableAliases.Select(alias => $"{alias}.*"));
            }
        }

        return string.Join(", ", tableAliases.Select(alias => $"{alias}.*"));
    }

    /// <summary>
    /// Create standard builders
    /// </summary>
    private static IReadOnlyDictionary<KsqlBuilderType, IKsqlBuilder> CreateStandardBuilders()
    {
        return new Dictionary<KsqlBuilderType, IKsqlBuilder>
        {
            [KsqlBuilderType.Join] = new JoinClauseBuilder(),
            [KsqlBuilderType.Select] = new SelectClauseBuilder(),
            [KsqlBuilderType.Where] = new WhereClauseBuilder(),
            [KsqlBuilderType.GroupBy] = new GroupByClauseBuilder(),
            [KsqlBuilderType.Having] = new HavingClauseBuilder(),
        };
    }

    /// <summary>
    /// Apply optimization hints
    /// </summary>
    protected override string ApplyOptimizationHints(string query, QueryAssemblyContext context)
    {
        var optimizedQuery = query;

        // Optimization hints specific to JOIN
        Console.WriteLine("[KSQL-LINQ HINT] Ensure joined tables are co-partitioned for optimal performance");

        if (query.Contains("LEFT JOIN"))
        {
            Console.WriteLine("[KSQL-LINQ HINT] LEFT JOIN may impact performance. Consider data denormalization if possible");
        }

        if (!context.IsPullQuery)
        {
            Console.WriteLine("[KSQL-LINQ HINT] Streaming JOINs require careful consideration of event-time vs processing-time semantics");
        }

        return optimizedQuery;
    }
}
