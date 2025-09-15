using Kafka.Ksql.Linq.Core.Modeling;
using Kafka.Ksql.Linq.Query.Abstractions;
using Kafka.Ksql.Linq.Query.Builders;
using Kafka.Ksql.Linq.Query.Builders.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Kafka.Ksql.Linq.Query.Pipeline;

/// <summary>
/// DML query generator (new builder version)
/// Rationale: separation-of-concerns; generate SELECT via integrated builders
/// </summary>
internal class DMLQueryGenerator : GeneratorBase, IDMLQueryGenerator
{
    /// <summary>
    /// Constructor (builder dependency injection)
    /// </summary>
    public DMLQueryGenerator(IReadOnlyDictionary<KsqlBuilderType, IKsqlBuilder> builders)
        : base(builders)
    {
    }

    /// <summary>
    /// Simplified constructor (uses standard builders)
    /// </summary>
    public DMLQueryGenerator() : this(CreateStandardBuilders())
    {
    }

    protected override KsqlBuilderType[] GetRequiredBuilderTypes()
    {
        return new[]
        {
            KsqlBuilderType.Where,
            KsqlBuilderType.Select
        };
    }

    /// <summary>
    /// Generate SELECT * query
    /// </summary>
    public string GenerateSelectAll(string objectName, bool isPullQuery = true, bool isTableQuery = false)
    {
        ModelCreatingScope.EnsureInScope();
        try
        {
            var context = new QueryAssemblyContext(objectName, isPullQuery, isTableQuery);
            var structure = CreateSelectStructure(objectName);

            var query = AssembleStructuredQuery(structure);
            return ApplyQueryPostProcessing(query, context);
        }
        catch (System.Exception ex)
        {
            return HandleGenerationError("SELECT ALL generation", ex, $"Object: {objectName}");
        }
    }

    /// <summary>
    /// Generate conditional SELECT query
    /// </summary>
    public string GenerateSelectWithCondition(string objectName, Expression whereExpression, bool isPullQuery = true, bool isTableQuery = false)
    {
        ModelCreatingScope.EnsureInScope();
        try
        {
            var context = new QueryAssemblyContext(objectName, isPullQuery, isTableQuery);
            var structure = CreateSelectStructure(objectName);

            // Append WHERE clause
            var whereContent = SafeCallBuilder(KsqlBuilderType.Where, whereExpression, "WHERE condition processing");
            var whereClause = QueryClause.Required(QueryClauseType.Where, $"WHERE {whereContent}", whereExpression);
            structure = structure.AddClause(whereClause);

            var query = AssembleStructuredQuery(structure);
            return ApplyQueryPostProcessing(query, context);
        }
        catch (System.Exception ex)
        {
            return HandleGenerationError("SELECT with condition generation", ex, $"Object: {objectName}");
        }
    }

    /// <summary>
    /// Generate COUNT query
    /// </summary>
    public string GenerateCountQuery(string objectName)
    {
        ModelCreatingScope.EnsureInScope();
        try
        {
            var context = new QueryAssemblyContext(objectName, true); // Pull Query
            var structure = CreateCountStructure(objectName);

            var query = AssembleStructuredQuery(structure);
            return ApplyQueryPostProcessing(query, context); // Post-processing also appends semicolons to COUNT queries
        }
        catch (System.Exception ex)
        {
            return HandleGenerationError("COUNT query generation", ex, $"Object: {objectName}");
        }
    }

    /// <summary>
    /// Generate aggregate query
    /// </summary>
    public string GenerateAggregateQuery(string objectName, Expression aggregateExpression)
    {
        ModelCreatingScope.EnsureInScope();
        try
        {
            var context = new QueryAssemblyContext(objectName, true); // Aggregates default to pull query
            var structure = CreateSelectStructure(objectName);

            // Handle aggregate expressions
            var selectContent = SafeCallBuilder(KsqlBuilderType.Select, aggregateExpression, "aggregate expression processing");
            var selectClause = QueryClause.Required(QueryClauseType.Select, selectContent, aggregateExpression);

            // Replace default SELECT *
            structure = structure.RemoveClause(QueryClauseType.Select);
            structure = structure.AddClause(selectClause);

            var query = AssembleStructuredQuery(structure);
            return ApplyQueryPostProcessing(query, context);
        }
        catch (System.Exception ex)
        {
            return HandleGenerationError("aggregate query generation", ex, $"Object: {objectName}");
        }
    }

    /// <summary>
    /// Generate complex LINQ query
    /// </summary>
    public string GenerateLinqQuery(string objectName, Expression linqExpression, bool isPullQuery = false, bool isTableQuery = false)
    {
        ModelCreatingScope.EnsureInScope();
        try
        {
            var context = new QueryAssemblyContext(objectName, isPullQuery, isTableQuery);
            var structure = CreateSelectStructure(objectName);

            // Analyze LINQ expression to build query clauses
            structure = ProcessLinqExpression(structure, linqExpression, context);

            var query = AssembleStructuredQuery(structure);
            return ApplyQueryPostProcessing(query, context);
        }
        catch (System.Exception ex)
        {
            return HandleGenerationError("LINQ query generation", ex, $"Object: {objectName}");
        }
    }

    /// <summary>
    /// Create basic SELECT structure
    /// </summary>
    private static QueryStructure CreateSelectStructure(string objectName)
    {
        var metadata = new QueryMetadata(DateTime.UtcNow, "DML");
        var structure = QueryStructure.CreateSelect(objectName).WithMetadata(metadata);

        // Add default SELECT * and FROM clauses
        var selectClause = QueryClause.Required(QueryClauseType.Select, "*");
        var fromClause = QueryClause.Required(QueryClauseType.From, $"FROM {objectName}");
        return structure.AddClauses(selectClause, fromClause);
    }

    /// <summary>
    /// Create COUNT structure
    /// </summary>
    private static QueryStructure CreateCountStructure(string objectName)
    {
        var metadata = new QueryMetadata(DateTime.UtcNow, "DML");
        var structure = QueryStructure.CreateSelect(objectName).WithMetadata(metadata);

        // Add COUNT(*) and FROM clauses
        var selectClause = QueryClause.Required(QueryClauseType.Select, "COUNT(*)");
        var fromClause = QueryClause.Required(QueryClauseType.From, $"FROM {objectName}");
        return structure.AddClauses(selectClause, fromClause);
    }

    /// <summary>
    /// Process LINQ expression
    /// </summary>
    private QueryStructure ProcessLinqExpression(QueryStructure structure, Expression linqExpression, QueryAssemblyContext context)
    {
        var analysis = AnalyzeLinqExpression(linqExpression);
        structure = structure.WithMetadata(analysis.ToMetadata());

        foreach (var methodCall in analysis.MethodCalls.AsEnumerable().Reverse())
        {
            structure = ProcessMethodCall(structure, methodCall, context);
        }

        return structure;
    }

    /// <summary>
    /// Process method call
    /// </summary>
    private QueryStructure ProcessMethodCall(QueryStructure structure, MethodCallExpression methodCall, QueryAssemblyContext context)
    {
        var methodName = methodCall.Method.Name;

        return methodName switch
        {
            "Select" => ProcessSelectMethod(structure, methodCall),
            "Where" => ProcessWhereMethod(structure, methodCall),
            "GroupBy" => ProcessGroupByMethod(structure, methodCall),
            "Having" => ProcessHavingMethod(structure, methodCall),
            "Join" => ProcessJoinMethod(structure, methodCall),
            "OrderBy" or "OrderByDescending" or "ThenBy" or "ThenByDescending" => ProcessOrderByMethod(structure, methodCall),
            "Take" => ProcessTakeMethod(structure, methodCall),
            "Skip" => ProcessSkipMethod(structure, methodCall),
            _ => structure // Ignore unsupported methods
        };
    }

    /// <summary>
    /// Handle SELECT method
    /// </summary>
    private QueryStructure ProcessSelectMethod(QueryStructure structure, MethodCallExpression methodCall)
    {
        if (methodCall.Arguments.Count >= 2)
        {
            var lambdaBody = ExtractLambdaBody(methodCall.Arguments[1]);
            if (lambdaBody != null)
            {
                var selectContent = SafeCallBuilder(KsqlBuilderType.Select, lambdaBody, "SELECT processing");
                var clause = QueryClause.Required(QueryClauseType.Select, selectContent, lambdaBody);

                // Replace existing SELECT clause
                structure = structure.RemoveClause(QueryClauseType.Select);
                structure = structure.AddClause(clause);
            }
        }

        return structure;
    }

    /// <summary>
    /// Handle WHERE method
    /// </summary>
    private QueryStructure ProcessWhereMethod(QueryStructure structure, MethodCallExpression methodCall)
    {
        if (methodCall.Arguments.Count >= 2)
        {
            var lambdaBody = ExtractLambdaBody(methodCall.Arguments[1]);
            if (lambdaBody != null)
            {
                bool treatAsHaving =
                    structure.HasClause(QueryClauseType.GroupBy) &&
                    HasAggregateFunction(lambdaBody);

                if (treatAsHaving)
                {
                    var havingContent = SafeCallBuilder(KsqlBuilderType.Having, lambdaBody, "HAVING processing");
                    var existingHaving = structure.GetClause(QueryClauseType.Having);
                    if (existingHaving != null)
                    {
                        var combinedContent = $"HAVING ({existingHaving.Content.Substring(7)}) AND ({havingContent})";
                        var combinedClause = QueryClause.Required(QueryClauseType.Having, combinedContent, lambdaBody);
                        structure = structure.RemoveClause(QueryClauseType.Having);
                        structure = structure.AddClause(combinedClause);
                    }
                    else
                    {
                        var havingClause = QueryClause.Required(QueryClauseType.Having, $"HAVING {havingContent}", lambdaBody);
                        structure = structure.AddClause(havingClause);
                    }
                }
                else
                {
                    var whereContent = SafeCallBuilder(KsqlBuilderType.Where, lambdaBody, "WHERE processing");

                    // Combine with existing WHERE clause (AND)
                    var existingWhere = structure.GetClause(QueryClauseType.Where);
                    if (existingWhere != null)
                    {
                        var combinedContent = $"WHERE ({existingWhere.Content.Substring(6)}) AND ({whereContent})";
                        var combinedClause = QueryClause.Required(QueryClauseType.Where, combinedContent, lambdaBody);
                        structure = structure.RemoveClause(QueryClauseType.Where);
                        structure = structure.AddClause(combinedClause);
                    }
                    else
                    {
                        var whereClause = QueryClause.Required(QueryClauseType.Where, $"WHERE {whereContent}", lambdaBody);
                        structure = structure.AddClause(whereClause);
                    }
                }
            }
        }

        return structure;
    }

    /// <summary>
    /// Handle GROUP BY method
    /// </summary>
    private QueryStructure ProcessGroupByMethod(QueryStructure structure, MethodCallExpression methodCall)
    {
        if (methodCall.Arguments.Count >= 2)
        {
            var lambdaBody = ExtractLambdaBody(methodCall.Arguments[1]);
            if (lambdaBody != null)
            {
                var groupByContent = SafeCallBuilder(KsqlBuilderType.GroupBy, lambdaBody, "GROUP BY processing");
                var clause = QueryClause.Required(QueryClauseType.GroupBy, $"GROUP BY {groupByContent}", lambdaBody);
                structure = structure.AddClause(clause);
            }
        }

        return structure;
    }

    /// <summary>
    /// Handle HAVING method
    /// </summary>
    private QueryStructure ProcessHavingMethod(QueryStructure structure, MethodCallExpression methodCall)
    {
        if (HasBuilder(KsqlBuilderType.Having) && methodCall.Arguments.Count >= 2)
        {
            var lambdaBody = ExtractLambdaBody(methodCall.Arguments[1]);
            if (lambdaBody != null)
            {
                var havingContent = SafeCallBuilder(KsqlBuilderType.Having, lambdaBody, "HAVING processing");
                var clause = QueryClause.Required(QueryClauseType.Having, $"HAVING {havingContent}", lambdaBody);
                structure = structure.AddClause(clause);
            }
        }

        return structure;
    }


    /// <summary>
    /// Handle ORDER BY method
    /// </summary>
    private QueryStructure ProcessOrderByMethod(QueryStructure structure, MethodCallExpression methodCall)
    {
        if (HasBuilder(KsqlBuilderType.OrderBy))
        {
            var orderByContent = SafeCallBuilder(KsqlBuilderType.OrderBy, methodCall, "ORDER BY processing");
            var clause = QueryClause.Optional(QueryClauseType.OrderBy, $"ORDER BY {orderByContent}", methodCall);

            // Replace existing to unify ORDER BY into a single clause
            structure = structure.RemoveClause(QueryClauseType.OrderBy);
            structure = structure.AddClause(clause);
        }

        return structure;
    }

    /// <summary>
    /// Handle TAKE method (LIMIT clause)
    /// </summary>
    private QueryStructure ProcessTakeMethod(QueryStructure structure, MethodCallExpression methodCall)
    {
        if (methodCall.Arguments.Count >= 2)
        {
            var limitValue = ExtractConstantValue(methodCall.Arguments[1]);
            var clause = QueryClause.Optional(QueryClauseType.Limit, $"LIMIT {limitValue}", methodCall);
            structure = structure.AddClause(clause);
        }

        return structure;
    }

    /// <summary>
    /// Handle SKIP method (warn since KSQL doesn't support it)
    /// </summary>
    private QueryStructure ProcessSkipMethod(QueryStructure structure, MethodCallExpression methodCall)
    {
        Console.WriteLine("[KSQL-LINQ WARNING] SKIP/OFFSET is not supported in KSQL. Use WHERE conditions for filtering instead.");
        return structure;
    }

    /// <summary>
    /// Handle JOIN method (supports simple inner JOIN only)
    /// </summary>
    private QueryStructure ProcessJoinMethod(QueryStructure structure, MethodCallExpression methodCall)
    {
        if (HasBuilder(KsqlBuilderType.Join))
        {
            var joinContent = SafeCallBuilder(KsqlBuilderType.Join, methodCall, "JOIN processing");
            var fromIndex = joinContent.IndexOf("FROM", StringComparison.OrdinalIgnoreCase);
            if (fromIndex >= 0)
            {
                var fromPart = joinContent.Substring(fromIndex); // FROM table JOIN ...
                var clause = QueryClause.Required(QueryClauseType.From, fromPart, methodCall);
                structure = structure.RemoveClause(QueryClauseType.From);
                structure = structure.AddClause(clause);
            }
        }

        return structure;
    }

    /// <summary>
    /// Analyze LINQ expression
    /// </summary>
    private ExpressionAnalysisResult AnalyzeLinqExpression(Expression expression)
    {
        var visitor = new MethodCallCollectorVisitor();
        visitor.Visit(expression);
        var result = visitor.Result;

        var selectCall = result.MethodCalls.FirstOrDefault(mc => mc.Method.Name == "Select");
        if (selectCall != null && selectCall.Arguments.Count > 1)
        {
            var body = ExtractLambdaBody(selectCall.Arguments[1]);
            if (body != null)
            {
                var ws = new WindowStartDetectionVisitor();
                ws.Visit(body);
                result.WindowStartCallCount = ws.Count;
                result.BucketColumnName = ws.ColumnName;
            }
        }

        var type = expression.Type.IsGenericType ? expression.Type.GetGenericArguments().FirstOrDefault() : null;
        if (type != null)
            result.PocoType = type;
        if (result.Windows.Count > 0)
        {
            if (result.TimeKey == null)
                throw new InvalidOperationException("Time key is required");
            if (string.IsNullOrEmpty(result.BucketColumnName))
                throw new InvalidOperationException("WindowStart() projection required for windowed queries");
            if (result.WindowStartCallCount != 1)
                throw new InvalidOperationException("Windowed query requires exactly one WindowStart() in projection.");
        }
        WindowValidator.Validate(result);
        return result;
    }

    /// <summary>
    /// Extract lambda body
    /// </summary>
    private static Expression? ExtractLambdaBody(Expression expression)
    {
        return BuilderValidation.ExtractLambdaBody(expression);
    }

    /// <summary>
    /// Extract constant value
    /// </summary>
    private static string ExtractConstantValue(Expression expression)
    {
        return expression switch
        {
            ConstantExpression constant => constant.Value?.ToString() ?? "0",
            UnaryExpression unary => ExtractConstantValue(unary.Operand),
            _ => "0"
        };
    }

    /// <summary>
    /// Check if expression contains aggregate functions
    /// </summary>
    private static bool HasAggregateFunction(Expression expression)
    {
        var visitor = new AggregateDetectionVisitor();
        visitor.Visit(expression);
        return visitor.HasAggregates;
    }

    /// <summary>
    /// Create standard builders
    /// </summary>
    private static IReadOnlyDictionary<KsqlBuilderType, IKsqlBuilder> CreateStandardBuilders()
    {
        return new Dictionary<KsqlBuilderType, IKsqlBuilder>
        {
            [KsqlBuilderType.Select] = new SelectClauseBuilder(),
            [KsqlBuilderType.Where] = new WhereClauseBuilder(),
            [KsqlBuilderType.GroupBy] = new GroupByClauseBuilder(forcePrefixAll: true),
            [KsqlBuilderType.Having] = new HavingClauseBuilder(),
            [KsqlBuilderType.Join] = new JoinClauseBuilder(),
            [KsqlBuilderType.OrderBy] = new OrderByClauseBuilder()
        };
    }

    /// <summary>
    /// Apply optimization hints
    /// </summary>
    protected override string ApplyOptimizationHints(string query, QueryAssemblyContext context)
    {
        var optimizedQuery = query;

        // Optimization for pull queries
        if (context.IsPullQuery)
        {
            // Index hint for pull queries
            if (query.Contains("WHERE") && !query.Contains("LIMIT"))
            {
                Console.WriteLine("[KSQL-LINQ HINT] Consider adding LIMIT clause for Pull Query performance");
            }
        }

        // Optimization for push queries
        if (!context.IsPullQuery)
        {
            // Performance hint for streaming queries
            if (query.Contains("ORDER BY"))
            {
                Console.WriteLine("[KSQL-LINQ WARNING] ORDER BY in Push Queries may impact performance. Consider using windowing.");
            }

            if (!query.Contains("EMIT CHANGES"))
            {
                optimizedQuery = ApplyQueryPostProcessing(optimizedQuery, context);
            }
        }

        return optimizedQuery;
    }
}
